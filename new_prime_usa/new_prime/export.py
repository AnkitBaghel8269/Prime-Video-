import html
import os.path

import pandas as pd
# import pyodbc  # Assuming you are using Microsoft SQL Server
import pymysql
from new_prime import db_config as db
# Set up the database connection

# Create a connection string
# DATABASE CONNECTION
connection = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, db=db.db_name)
cursor = connection.cursor()

# SQL query to fetch data
sql_query = "SELECT * FROM series_data_usa"


# Fetch data into a Pandas DataFrame
df = pd.read_sql(sql_query, connection)
def decode_unicode(text):
    return text.encode('utf-8').decode('unicode_escape')

df = df.applymap(lambda x: html.unescape(x).strip() if isinstance(x, str) else x)

df['starring']  = df['starring'].apply(decode_unicode)
# Close the database connection

filepath = "F:/Ankit_Live/ott_output_file/05-03-2024/amazon_series"
if not os.path.exists(filepath):
    os.makedirs(filepath)
# Specify the Excel file path
excel_file_path = f'{filepath}/series_data_usa_20240307.xlsx'

# Export DataFrame to Excel
# df.to_excel(excel_file_path, index=False)
writer = pd.ExcelWriter(
   f'{excel_file_path}',
   engine='xlsxwriter',
   engine_kwargs={'options': {'strings_to_urls': False}}
 )


writer.book.use_zip64()
df.to_excel(writer)
writer.close()

print(f'Data exported to {excel_file_path}')
