"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

employees_data = "./north_data/employees_data.csv"
customers_data = "./north_data/customers_data.csv"
orders_data = "./north_data/orders_data.csv"

conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="9999")
try:
    with conn:
        with conn.cursor() as cur:

            with open(customers_data, newline='', encoding='utf-8') as csvfile:
                read = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(read)
                for row in read:
                    cur.execute('INSERT INTO customers VALUES (%s, %s, %s)', row)

            with open(employees_data, newline='', encoding='utf-8') as csvfile:
                i = 0
                read = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(read)
                for row in read:
                    i += 1
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", [i] + row)

            with open('north_data/orders_data.csv', newline='', encoding='utf-8') as csvfile:
                read = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(read)
                for row in read:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", row)

finally:
    conn.close()



