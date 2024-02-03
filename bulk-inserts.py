import mysql.connector
import random
import string
import hashlib


def random_string(length):
    letters = string.ascii_lowercase
    dig = string.digits
    result_str = ''.join(random.choices(dig + letters, k=length))
    return result_str


def random_letters(length):
    letter = string.ascii_lowercase
    result_str = ''.join(random.choice(letter) for i in range(length))
    return result_str


def random_digits(length):
    dig = string.digits
    result_str = ''.join(random.choice(dig) for i in range(length))
    return result_str


def hash_password(password):
    password_bytes = password.encode('utf-8')
    hash_object = hashlib.sha256(password_bytes)
    return hash_object.hexdigest()


def useraccount_bulk_random_insert(connection, cursor):
    useraccount_insert_query = """INSERT INTO useraccount 
    (username, password, firstname, lastname, email, address, phone_number, bank_account_number, registration_date) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
    user_id = 1000000
    for i in range(100000):
        record = (f'usr{user_id}', hash_password(random_string(8)), random_letters(5), random_letters(5),
                  f'{random_string(8)}@gmail.com', f'{random_letters(4)}_{random_letters(4)}_{random_letters(3)}',
                  f'{random_digits(3)}-{random_digits(3)}-{random_digits(3)}',
                  f'{random_digits(4)}-{random_digits(4)}-{random_digits(4)}-{random_digits(4)}',
                  f'{random.randint(2015, 2021)}-{random.randint(1, 12)}-{random.randint(1, 28)}')
        cursor.execute(useraccount_insert_query, record)
        connection.commit()
        user_id = user_id + 1


def digitalWallet_bulk_random_insert(connection, cursor):
    digitalWallet_query = """INSERT INTO digitalwallet (wallet_id, wallet_pass, username) 
                                        VALUES (%s, %s, %s) """
    wallet_id = 2000000
    user_id = 1000000
    for i in range(100000):
        record = (f'wall{wallet_id}', hash_password(random_string(8)), f'usr{user_id}')
        cursor.execute(digitalWallet_query, record)
        connection.commit()
        wallet_id = wallet_id + 1
        user_id = user_id + 1


def currency_insert(connection, cursor):
    currency_query = 'INSERT INTO currency (currency_id, currency_name) VALUES (%s, %s)'

    record = ("1", "toman")
    cursor.execute(currency_query, record)
    connection.commit()

    record = ("2", "dollar")
    cursor.execute(currency_query, record)
    connection.commit()

    record = ("3", "euro")
    cursor.execute(currency_query, record)
    connection.commit()


def market_insert(connection, cursor):
    market_query = 'INSERT INTO market (market_id, market_name) VALUES (%s, %s)'
    record = ("1", "dollar-toman")
    cursor.execute(market_query, record)
    connection.commit()

    record = ("2", "dollar-euro")
    cursor.execute(market_query, record)
    connection.commit()

    record = ("3", "euro-toman")
    cursor.execute(market_query, record)
    connection.commit()


def currency_exchange_rate_insert(connection, cursor):
    exchange_rate_query = """INSERT INTO currency_exchange_rate (market_id, currency_id, exchange_rate)
                                VALUES (%s, %s, %s)"""
    record = ('1', '1', 0.000017)
    cursor.execute(exchange_rate_query, record)
    connection.commit()

    record = ('1', '2', 60000.0)
    cursor.execute(exchange_rate_query, record)
    connection.commit()

    record = ('2', '2', 0.92)
    cursor.execute(exchange_rate_query, record)
    connection.commit()

    record = ('2', '3', 1.09)
    cursor.execute(exchange_rate_query, record)
    connection.commit()

    record = ('3', '1', 0.000015)
    cursor.execute(exchange_rate_query, record)
    connection.commit()

    record = ('3', '3', 65000.0)
    cursor.execute(exchange_rate_query, record)
    connection.commit()


def currency_balance_bulk_random_insert(connection, cursor):
    currency_balance_query = """INSERT INTO currency_balance (wallet_id, currency_id, balance) 
                                        VALUES (%s, %s, %s) """

    wallet_id = 2000000
    for i in range(100000):

        record = (f'wall{wallet_id}', '1', random.randint(0, 1000000))
        cursor.execute(currency_balance_query, record)
        connection.commit()

        record = (f'wall{wallet_id}', '2', random.randint(0, 1000))
        cursor.execute(currency_balance_query, record)
        connection.commit()

        record = (f'wall{wallet_id}', '3', random.randint(0, 1000))
        cursor.execute(currency_balance_query, record)
        connection.commit()

        wallet_id = wallet_id + 1


def marketTransaction_bulk_insert(connection, cursor):
    marketTransaction_query = """INSERT INTO markettransaction 
    (date, time, market_id, username, exchanged_currency, exchanged_amount, paid_with_currency, price, wallet_id) 
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """

    wallet_id = 2000000
    user_id = 1000000
    for i in range(100000):
        transaction_num = random.randint(0, 15)
        for j in range(transaction_num):
            market_id = random.randint(1, 3)

            if market_id == 1:
                currencies = [2, 1]
                rates = [60000.0, 0.000017]
                rand_currency = random.randint(0, 1)
                exchanged_currency = currencies.pop(rand_currency)
                paid_with_currency = currencies.pop()
                exchanged_amount = random.randint(1, 1000)
                price = exchanged_amount * rates[rand_currency]

            elif market_id == 2:
                currencies = [2, 3]
                rates = [0.92, 1.09]
                rand_currency = random.randint(0, 1)
                exchanged_currency = currencies.pop(rand_currency)
                paid_with_currency = currencies.pop()
                exchanged_amount = random.randint(1, 1000)
                price = exchanged_amount * rates[rand_currency]

            else:
                currencies = [3, 1]
                rates = [65000.0, 0.000015]
                rand_currency = random.randint(0, 1)
                exchanged_currency = currencies.pop(rand_currency)
                paid_with_currency = currencies.pop()
                exchanged_amount = random.randint(1, 1000)
                price = exchanged_amount * rates[rand_currency]

            record = (f'{random.randint(2022, 2023)}-{random.randint(1, 12)}-{random.randint(1, 28)}',
                      f'{random.randint(0, 23)}:{random.randint(0, 59)}', str(market_id), f'usr{user_id}',
                      str(exchanged_currency), exchanged_amount, str(paid_with_currency), price, f'wall{wallet_id}')
            cursor.execute(marketTransaction_query, record)
            connection.commit()

        wallet_id = wallet_id + 1
        user_id = user_id + 1


def main():
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='Exchange',
                                             user='root',
                                             password='34129093428198matin')
        cursor = connection.cursor()

        # useraccount_bulk_random_insert(connection, cursor)
        # digitalWallet_bulk_random_insert(connection, cursor)
        # currency_insert(connection, cursor)
        # currency_balance_bulk_random_insert(connection, cursor)
        # market_insert(connection, cursor)
        # currency_exchange_rate_insert(connection, cursor)
        # marketTransaction_bulk_insert(connection, cursor)

    except mysql.connector.Error as error:
        print("Failed to insert into MySQL table {}".format(error))

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


if __name__ == '__main__':
    main()