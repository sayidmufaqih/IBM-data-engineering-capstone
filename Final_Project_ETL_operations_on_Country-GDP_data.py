# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    log_file = "code_log.txt"
    with open(log_file,"a") as f: 
        f.write(timestamp + ':' + message + '\n')

def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    table = data.find('table', attrs=table_attribs)
    headers = [header.text.strip() for header in table.find_all('th')]
    table_data = []
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        row_data= [cell.get_text(strip=True) for cell in cells]
        if cells:
            table_data.append(row_data)
    if headers:
        df = pd.DataFrame(table_data, columns=headers)
    else:
        df = pd.DataFrame(table_data)
    df = df.drop("Rank", axis=1)
    df.rename(columns={'Market cap(US$ billion)': 'MC_USD_Billion', 'Bank name': 'Name'}, inplace=True)
  # Change Market cap(US$ billion column data type to float
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)
    return df


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = {"class" : "wikitable sortable mw-collapsible"} 
extracted_data = extract(url, table_attribs)
print(extracted_data)
log_progress("Data extraction complete. Initiating Transformation process")

def transform(df, csv_path):
    # Read the exchange rates from the CSV file
    exchange_rates = pd.read_csv(csv_path)
	
	# Create a dictionary of exchange rates
    rates_dict = dict(zip(exchange_rates['Currency'], exchange_rates['Rate']))
	
	# Convert Market Cap to each currency and add new columns
    for currency, rate in rates_dict.items():
        column_name = f'MC_{currency}_Billion'
        df[column_name] = round(df['MC_USD_Billion'] * rate, 2)
    return df

csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
df = extracted_data
transformed_data = transform(df, csv_path) 
print("Transformed Data") 
print(transformed_data)
print(transformed_data['MC_EUR_Billion'][4])
log_progress("Data transformation complete. Initiating Loading process")

def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

output_path = "./Largest_banks_data.csv"
load_to_csv(df, output_path)
log_progress("Data saved to CSV file")

log_progress("SQL Connection initiated")
def load_to_db(df, sql_connection, table_name):
	# Load the df to the database
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)
    print('Table is ready')

sql_connection = sqlite3.connect('Banks.db')
table_name = 'Largest_banks'
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

def run_query(query_statements, sql_connection):
    for query_statement in query_statements:
        query_output = pd.read_sql(query_statement, sql_connection)
        print(f"Executing query: {query_statement}")
        print(query_output)

query_statements = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "SELECT Name FROM Largest_banks LIMIT 5"
]
run_query(query_statements, sql_connection)
log_progress("Process Complete")
log_progress("Server Connection closed")
