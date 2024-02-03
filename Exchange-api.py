from flask import Flask, request
import mysql.connector
import collections
import json
import hashlib
from datetime import date
from datetime import datetime


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
        # puts the data in json data type
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

        query = """SELECT useraccount.username, useraccount.firstname, useraccount.lastname,
                    c1.currency_name, SUM(markettransaction.exchanged_amount) AS total_exchanged_amount
                    FROM useraccount, markettransaction, currency AS c1, currency AS c2
                    WHERE useraccount.username = markettransaction.username
                    AND markettransaction.exchanged_currency = c1.currency_id
                    AND markettransaction.paid_with_currency = c2.currency_id
                    AND c1.currency_name = "dollar" AND c2.currency_name = "toman"
                    AND YEAR(markettransaction.date) = "2022"
                    GROUP BY useraccount.username, useraccount.firstname, useraccount.lastname, c1.currency_name
                    ORDER BY SUM(markettransaction.exchanged_amount) DESC
                    LIMIT 5"""

        cursor.execute(query)
        rows = cursor.fetchall()
        # puts the data in json data type
        objects = []
        for row in rows:
            d = collections.OrderedDict()
            d['username'] = row[0]
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

        # checks if the credentials are correct
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
        # checks if the username already exists
        check_query = 'SELECT * FROM useraccount WHERE username = %s'
        cursor.execute(check_query, (username,))
        result = cursor.fetchone()
        if result is not None:
            return f"Username {username} already exists! Try another"

        temp = request.json["password"]
        # checks password length
        if len(temp) < 8:
            return "Your password must be at least 8 characters or digits"
        password_bytes = temp.encode('utf-8')
        hash_object = hashlib.sha256(password_bytes)
        password = hash_object.hexdigest()

        firstname = request.json["firstname"]
        lastname = request.json["lastname"]

        email = request.json["email"]
        # checks if the email already exists
        check_query = 'SELECT * FROM useraccount WHERE email = %s'
        cursor.execute(check_query, (email,))
        result = cursor.fetchone()
        if result is not None:
            return f"Email {email} is already in use! Try another"

        address = request.json["address"]
        phone_number = request.json["phone_number"]

        bank_account_number = request.json["bank_account_number"]
        # checks if the bank account number already exists
        check_query = 'SELECT * FROM useraccount WHERE bank_account_number = %s'
        cursor.execute(check_query, (bank_account_number,))
        result = cursor.fetchone()
        if result is not None:
            return f"bank-account number {bank_account_number} is already in use! Try another"

        registration_date = date.today()

        # inserts new user account
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

        # checks if the credentials are correct
        query = 'SELECT * FROM useraccount WHERE username = %s AND password = %s'
        record = (username, password)
        cursor.execute(query, record)
        result = cursor.fetchone()
        if result is None:
            return "Delete account failed! Wrong credentials"
        else:
            # deletes the user account
            query = 'DELETE FROM useraccount WHERE username = %s AND password = %s'
            cursor.execute(query, record)
            connection.commit()
            return f"Your account '{username}' has been deleted"

    except mysql.connector.Error as error:
        return "Failed to delete from MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


@app.route('/change-password', methods=['POST'])
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

        # checks if the credentials are correct
        check_query = 'SELECT * FROM useraccount WHERE username = %s AND password = %s'
        cursor.execute(check_query, (username, old_password))
        result = cursor.fetchone()
        if result is None:
            return "Wrong password or username!"

        # updates password
        else:
            temp = request.json["new_password"]
            password_bytes = temp.encode('utf-8')
            hash_object = hashlib.sha256(password_bytes)
            new_password = hash_object.hexdigest()
            update_query = 'UPDATE useraccount SET password = %s WHERE username = %s AND password = %s'
            record = (new_password, username, old_password)
            cursor.execute(update_query, record)
            connection.commit()
            return "Your password has been updated!"

    except mysql.connector.Error as error:
        return "Failed to update MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


@app.route('/create-wallet', methods=['POST'])
def create_wallet():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='admin',
                                             password='admin')
        cursor = connection.cursor()

        username = request.json['username']

        temp = request.json['password']
        password_bytes = temp.encode('utf-8')
        hash_object = hashlib.sha256(password_bytes)
        password = hash_object.hexdigest()

        check_query = 'SELECT * FROM useraccount WHERE username = %s AND password = %s'
        cursor.execute(check_query, (username, password))
        result = cursor.fetchone()
        if result is None:
            return "Username or password is wrong"
        else:
            # checks if the wallet_id already exists
            wallet_id = request.json['wallet_id']
            cursor.execute('SELECT * FROM digitalwallet WHERE wallet_id = %s', (wallet_id,))
            result = cursor.fetchone()
            if result is not None:
                return "The wallet id you entered already exists! Try another"

            temp = request.json['wallet_pass']
            password_bytes = temp.encode('utf-8')
            hash_object = hashlib.sha256(password_bytes)
            wallet_pass = hash_object.hexdigest()

            # inserts new digital wallet
            insert_query = 'INSERT INTO digitalwallet (wallet_id, wallet_pass, username) VALUES (%s, %s, %s)'
            record = (wallet_id, wallet_pass, username)
            cursor.execute(insert_query, record)
            connection.commit()

            # gets currency ids to insert initial balance for each currency of the new wallet
            cursor.execute('SELECT currency_id FROM currency')
            result = cursor.fetchall()
            currency_ids = []
            for i in result:
                currency_ids.append(i[0])

            insert_query = 'INSERT INTO currency_balance (wallet_id, currency_id, balance) VALUES (%s, %s, 0.0)'
            for currency_id in currency_ids:
                record = (wallet_id, currency_id)
                cursor.execute(insert_query, record)
                connection.commit()

            return f"New wallet created!"

    except mysql.connector.Error as error:
        return "Failed to insert into MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


@app.route('/open-wallet', methods=['POST'])
def open_wallet():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='report',
                                             password='report')
        cursor = connection.cursor()

        wallet_id = request.json['wallet_id']

        temp = request.json['wallet_pass']
        password_bytes = temp.encode('utf-8')
        hash_object = hashlib.sha256(password_bytes)
        wallet_pass = hash_object.hexdigest()

        # checks if the credentials are correct
        check_query = 'SELECT * FROM digitalwallet WHERE wallet_id = %s AND wallet_pass = %s'
        cursor.execute(check_query, (wallet_id, wallet_pass))
        result = cursor.fetchone()
        if result is None:
            'Your wallet id or password is wrong'
        else:
            # fetches wallet's currencies and their balances
            select_balance = """SELECT currency_balance.wallet_id, currency.currency_name, currency_balance.balance
                                FROM currency_balance, currency
                                WHERE currency_balance.currency_id = currency.currency_id
                                AND currency_balance.wallet_id = %s"""
            cursor.execute(select_balance, (wallet_id,))
            rows = cursor.fetchall()
            objects = []
            for row in rows:
                d = collections.OrderedDict()
                d['wallet_id'] = row[0]
                d['currency_name'] = row[1]
                d['balance'] = row[2]
                objects.append(d)

            j = json.dumps(objects)
            return j

    except mysql.connector.Error as error:
        return "Failed to select from MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


@app.route('/charge-wallet', methods=['POST'])
def charge_wallet():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='admin',
                                             password='admin')
        cursor = connection.cursor()

        wallet_id = request.json['wallet_id']

        temp = request.json['wallet_pass']
        password_bytes = temp.encode('utf-8')
        hash_object = hashlib.sha256(password_bytes)
        wallet_pass = hash_object.hexdigest()

        charge_amount = request.json['charge_amount']

        # checks if the credentials are correct
        check_query = 'SELECT * FROM digitalwallet WHERE wallet_id = %s AND wallet_pass = %s'
        cursor.execute(check_query, (wallet_id, wallet_pass))
        result = cursor.fetchone()
        if result is None:
            'Your wallet id or password is wrong'
        else:
            # updates balance of toman currency
            update_balance = """UPDATE currency_balance SET balance = %s
                                WHERE currency_balance.currency_id =
                                (SELECT currency_id FROM currency WHERE currency_name = "toman")"""
            cursor.execute(update_balance, (float(charge_amount),))
            connection.commit()
            return "Your wallet charged successfully!"

    except mysql.connector.Error as error:
        return "Failed to update MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


@app.route('/transaction', methods=['POST'])
def transaction():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='admin',
                                             password='admin')
        cursor = connection.cursor()

        trans_date = date.today()
        temp = datetime.now()
        trans_time = temp.strftime('%H:%M:%S')

        # fetches market id
        market = request.json['market']
        cursor.execute('SELECT market_id FROM market WHERE market_name = %s', (market,))
        result = cursor.fetchone()
        market_id = result[0]

        username = request.json['username']

        # fetches currency id
        select_id = 'SELECT currency_id FROM currency WHERE currency_name = %s'
        exchanged_currency_name = request.json['exchanged_currency']
        cursor.execute(select_id, (exchanged_currency_name,))
        result = cursor.fetchone()
        exchanged_currency = result[0]

        paid_with_currency_name = request.json['paid_with_currency']
        cursor.execute(select_id, (paid_with_currency_name,))
        result = cursor.fetchone()
        paid_with_currency = result[0]

        exchanged_amount = float(request.json['exchanged_amount'])

        # calculates price based on exchange_rate in market
        select_rate_query = """SELECT exchange_rate FROM currency_exchange_rate
                                WHERE market_id = %s AND currency_id = %s"""
        cursor.execute(select_rate_query, (market_id, exchanged_currency))
        result = cursor.fetchone()
        rate = result[0]
        price = exchanged_amount * float(rate)

        wallet_id = request.json['wallet_id']

        # checks if the user has enough balance
        check_query = 'SELECT balance FROM currency_balance WHERE wallet_id = %s AND currency_id = %s'
        cursor.execute(check_query, (wallet_id, paid_with_currency))
        result = cursor.fetchone()
        balance = float(result[0])
        if balance < price:
            return "Trade failed! You don't have enough balance!"

        else:
            # inserts transaction
            insert_trans = """INSERT INTO markettransaction 
            (date, time, market_id, username, exchanged_currency, exchanged_amount, paid_with_currency, price, wallet_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            record = (trans_date, trans_time, market_id, username, exchanged_currency,
                      exchanged_amount, paid_with_currency, price, wallet_id)
            cursor.execute(insert_trans, record)
            connection.commit()

            # updates user's wallet currency balances
            update_exchanged_balance = """UPDATE currency_balance SET balance = balance + %s
                                            WHERE wallet_id = %s AND currency_id = %s"""
            update_paid_balance = """UPDATE currency_balance SET balance = balance - %s
                                            WHERE wallet_id = %s AND currency_id = %s"""
            cursor.execute(update_exchanged_balance, (exchanged_amount, wallet_id, exchanged_currency))
            connection.commit()
            cursor.execute(update_paid_balance, (price, wallet_id, paid_with_currency))
            connection.commit()
            return "Trade was successful!"

    except mysql.connector.Error as error:
        return "Failed to operate on MySQL table {}".format(error)

    finally:
        cursor.close()
        connection.close()


if __name__ == '__main__':
    app.run(debug=True, port=8000)
