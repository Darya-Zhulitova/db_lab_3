import os
from time import perf_counter
from statistics import median
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import duckdb


def print_time(cursor, queries, pandas=False):
    for i in range(len(queries)):
        arr = []
        for j in range(15):
            tick = perf_counter()
            if pandas:
                pd.read_sql(queries[i], con=cursor)
            else:
                cursor.execute(queries[i])
            tock = perf_counter()
            arr.append(tock - tick)
        print("Query", i + 1, "time", median(arr))


queries = [
    'SELECT "VendorID", count(*) FROM nyc GROUP BY 1;',
    'SELECT passenger_count, avg(total_amount) FROM nyc GROUP BY 1;',
    'SELECT passenger_count, extract(year from tpep_pickup_datetime::date), count(*) FROM nyc GROUP BY 1, 2;',
    'SELECT passenger_count, extract(year from tpep_pickup_datetime::date), round(trip_distance), count(*) FROM nyc GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;']

queries_sqlite = [
    'SELECT "VendorID", count(*) FROM nyc GROUP BY 1;',
    'SELECT passenger_count, avg(total_amount) FROM nyc GROUP BY 1;',
    '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS "year", count(*) FROM nyc GROUP BY 1, 2;''',
    '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) AS "year", round(trip_distance), count(*) FROM nyc GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;''']

csv_path = "nyc_yellow_tiny.csv"

db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

if not os.path.exists("databases"):
    os.makedirs("databases")

# Postgres import csv
try:
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
    df = pd.read_csv(csv_path)
    df.to_sql('nyc', engine, if_exists='replace', index=False)
except psycopg2.Error as e:
    print("Ошибка подключения к базе данных:", e)


# Postgres
try:
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    print("Postgres:")
    print_time(cursor, queries)
    cursor.close()
    connection.close()
except psycopg2.Error as e:
    print("Ошибка подключения к базе данных:", e)


# SQLite
with sqlite3.connect("databases/nyc.db") as conn:
    cursor = conn.cursor()
    df = pd.read_csv(csv_path)
    df.to_sql('nyc', conn, if_exists='replace', index=False)
    print("SQLite:")
    print_time(cursor, queries_sqlite)
    conn.commit()

# DuckDB
conn = duckdb.connect()
conn.execute(f'CREATE TABLE nyc AS FROM {csv_path};')
print("DuckDB:")
print_time(conn, queries)
conn.close()

# Pandas
engine = create_engine('sqlite:///databases/nyc_pd.db')
df = pd.read_csv(csv_path)
df.to_sql('nyc', engine, if_exists='replace', index=False)
print("Pandas:")
print_time(engine, queries_sqlite, pandas=True)
