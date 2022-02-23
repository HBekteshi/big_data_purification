import sqlite3
from sqlite3 import Error
from cleanco import basename, prepare_terms
import pandas as pd
import csv


# 1.Connecting to the database
def database_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print("Cannot connect to the database: " + str(e))

    return conn


# 2.Reading database(in chunks of certain rows because of memory optimization)
def read_database(conn, begin, end):
    cur = conn.cursor()
    cur.execute("SELECT * FROM companies WHERE ROWID BETWEEN ? and ?;", [begin, end])

    rows = cur.fetchall()

    for row in rows:
        print(row)


# 3.Cleaning Data from parenthesis, commas and abbreviations
def data_clean(csv_file, column_name):
    df = pd.read_csv(csv_file)
    terms = prepare_terms()

    # Cleaning words that begin(and end) with parenthesis and commas
    df[column_name] = df[column_name].str.replace(r"\(.*", "")
    df[column_name] = df[column_name].str.replace(r"\,.*", "")

    # Cleaning names from Limited, LTD, LCC abbreviations
    for index, val in enumerate(df[column_name]):
        val = basename(str(val), terms, prefix=False, middle=False, suffix=True)
        df[column_name][index] = val

    df.to_csv(csv_file, index=False)


# 4. Normalizing the cleaned columns. Capitalizing company names
def normalize_data(csv_file, column_name):
    df = pd.read_csv(csv_file)

    # Names like: MCL CONSTRUCTION'S GROUP will not be converted to : MCL Construction's Group,
    # but to: Mcl Construction'S Group

    df[column_name] = df[column_name].str.title()
    df.to_csv(csv_file, index=False)


# 5. Updating database columns by inserting columns from the cleaned CSV file
def update_database(conn, csv_file):
    cur = conn.cursor()

    with open(csv_file, 'rt') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['company_name_cleaned'], i['id'], i['name'],) for i in dr]

    cur.executemany("UPDATE companies SET company_name_cleaned = ? WHERE id = ? AND name = ?", to_db)
    # print(cur.execute("SELECT * FROM companies").fetchall())

    conn.commit()


# 6. Closing database connection (recommended)
def close_connection(db_file):
    con = database_connection(db_file)
    try:
        if con:
            con.close()
        print("Connection closed")
    except sqlite3.Error as e:
        print("Connection could not be closed: ", e)


database_name = 'semos_company_names.db'
csv_file_name = 'companies.csv'
row_name = 'company_name_cleaned'

# 1.Function call
# database_connection(database_name)

# 2.Function call
# read_database(database_connection(database_name), 10, 20)

# 3.Function call
# data_clean(csv_file_name, row_name)

# 4.Function call
# normalize_data(csv_file_name, row_name)

# 5.Function call
# update_database(database_connection(database_name), csv_file_name)

# 6.Function call
# close_connection(database_name)
