from flask import Flask, jsonify, request
import mysql.connector
import collections
import json
import hashlib


app = Flask(__name__)


# monthly summery of the total amount of each currency exchanged
# and number of transactions for each currency
@app.route('/report/monthly-summery', methods=['GET'])
def monthly_summery():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='report',
                                             password='report')
        cursor = connection.cursor()

        query = """SELECT YEAR(markettransaction.date), MONTH(markettransaction.date),
                    currency.currency_name, SUM(markettransaction.exchanged_amount) AS total_exchanged_amount,
                    COUNT(*) AS number_of_transactions
                    FROM markettransaction, currency
                    WHERE markettransaction.exchanged_currency = currency.currency_id
                    GROUP BY YEAR(markettransaction.date), MONTH(markettransaction.date), currency.currency_name
                    ORDER BY YEAR(markettransaction.date), MONTH(markettransaction.date), currency.currency_name"""

        cursor.execute(query)
        rows = cursor.fetchall()
        objects = []
        for row in rows:
            d = collections.OrderedDict()
            d['year'] = row[0]
            d['month'] = row[1]
            d['currency_name'] = row[2]
            d['total_exchanged_amount'] = row[3]
            d['number_of_transactions'] = row[4]
            objects.append(d)

        j = json.dumps(objects)
        return j

    except mysql.connector.Error as error:
        return "Failed to select from MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


# top 5 traders of the last year
@app.route('/report/top5-traders', methods=['GET'])
def top5_traders():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='report',
                                             password='report')
        cursor = connection.cursor()

        query = """SELECT useraccount.user_id, useraccount.firstname, useraccount.lastname,
                    c1.currency_name, SUM(markettransaction.exchanged_amount) AS total_exchanged_amount
                    FROM useraccount, markettransaction, currency AS c1, currency AS c2
                    WHERE useraccount.user_id = markettransaction.user_id 
                    AND markettransaction.exchanged_currency = c1.currency_id
                    AND markettransaction.paid_with_currency = c2.currency_id
                    AND c1.currency_name = "dollar" AND c2.currency_name = "toman"
                    AND YEAR(markettransaction.date) = "2022"
                    GROUP BY useraccount.user_id, useraccount.firstname, useraccount.lastname, c1.currency_name
                    ORDER BY SUM(markettransaction.exchanged_amount) DESC
                    LIMIT 5"""

        cursor.execute(query)
        rows = cursor.fetchall()
        objects = []
        for row in rows:
            d = collections.OrderedDict()
            d['user_id'] = row[0]
            d['firstname'] = row[1]
            d['lastname'] = row[2]
            d['currency_name'] = row[3]
            d['total_exchanged_amount'] = row[4]
            objects.append(d)

        j = json.dumps(objects)
        return j

    except mysql.connector.Error as error:
        return "Failed to select from MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


# checks if the entered user credentials exists. If so, user logs in
@app.route('/login', methods=['POST'])
def login():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='usermanager',
                                             password='usermanager')
        cursor = connection.cursor()
        username = request.json["username"]

        temp = request.json["password"]
        password_bytes = temp.encode('utf-8')
        hash_object = hashlib.sha256(password_bytes)
        password = hash_object.hexdigest()

        query = 'SELECT * FROM useraccount WHERE username = %s AND password = %s'
        record = (username, password)
        cursor.execute(query, record)
        result = cursor.fetchone()
        if result is None:
            return "Wrong password or username"
        else:
            return "You're logged in!"

    except mysql.connector.Error as error:
        return "Failed to select from MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


# signs up user's data as a new user
@app.route('/signup', methods=['POST'])
def signup():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='usermanager',
                                             password='usermanager')
        cursor = connection.cursor()

        username = request.json["username"]
        check_query = 'SELECT * FROM useraccount WHERE username = %s'
        cursor.execute(check_query, (username,))
        result = cursor.fetchone()
        if result is not None:
            return f"Username {username} already exists! Try another"

        temp = request.json["password"]
        if len(temp) < 8:
            return "Your password must be at least 8 characters or digits"
        password_bytes = temp.encode('utf-8')
        hash_object = hashlib.sha256(password_bytes)
        password = hash_object.hexdigest()

        firstname = request.json["firstname"]
        lastname = request.json["lastname"]

        email = request.json["email"]
        check_query = 'SELECT * FROM useraccount WHERE email = %s'
        cursor.execute(check_query, (email,))
        result = cursor.fetchone()
        if result is not None:
            return f"Email {email} is already in use! Try another"

        address = request.json["address"]
        phone_number = request.json["phone_number"]
        bank_account_number = request.json["bank_account_number"]
        registration_date = request.json["registration_date"]

        query = """INSERT INTO useraccount
        (username, password, firstname, lastname, email,
        address, phone_number, bank_account_number, registration_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        record = (username, password, firstname, lastname,
                  email, address, phone_number, bank_account_number, registration_date)
        cursor.execute(query, record)
        connection.commit()
        return "You signed up successfully!"

    except mysql.connector.Error as error:
        return "Failed to insert into MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


# deletes user's account
@app.route('/delete-account', methods=['POST'])
def delete_account():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='usermanager',
                                             password='usermanager')
        cursor = connection.cursor()

        username = request.json["username"]

        temp = request.json["password"]
        password_bytes = temp.encode('utf-8')
        hash_object = hashlib.sha256(password_bytes)
        password = hash_object.hexdigest()

        query = 'SELECT * FROM useraccount WHERE username = %s AND password = %s'
        record = (username, password)
        cursor.execute(query, record)
        result = cursor.fetchone()
        if result is None:
            return "Delete account failed! Wrong credentials"
        else:
            query = 'DELETE FROM useraccount WHERE username = %s AND password = %s'
            cursor.execute(query, record)
            connection.commit()
            return f"Your account '{username}' has been deleted"

    except mysql.connector.Error as error:
        return "Failed to delete from MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


@app.route('/change-password', method=['POST'])
def change_password():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='usermanager',
                                             password='usermanager')
        cursor = connection.cursor()

        username = request.json["username"]
        temp = request.json["old_password"]
        password_bytes = temp.encode('utf-8')
        hash_object = hashlib.sha256(password_bytes)
        old_password = hash_object.hexdigest()

        check_query = 'SELECT * FROM useraccount WHERE username = %s AND password = %s'
        cursor.execute(check_query, (username, old_password))
        result = cursor.fetchone()
        if result is None:
            return "Wrong password or username!"

        else:
            temp = request.json["new_password"]
            password_bytes = temp.encode('utf-8')
            hash_object = hashlib.sha256(password_bytes)
            new_password = hash_object.hexdigest()
            update_query = 'UPDATE TABLE useraccount SET password = %s WHERE username = %s AND password = %s'
            record = (new_password, username, old_password)
            cursor.execute(update_query, record)
            connection.commit()
            return "Your password has been updated!"

    except mysql.connector.Error as error:
        return "Failed to update MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


if __name__ == '__main__':
    app.run(debug=True, port=8000)