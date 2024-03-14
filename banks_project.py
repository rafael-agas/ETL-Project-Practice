from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 

def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    rows = soup.find_all("tbody")[0].find_all("tr")
    data_list = []
    for row in rows:
        if row.find('td') is not None:
            td = row.find_all("td")
            data_dict = {"Name":td[1].find_all('a')[1].contents[0],
                        "MC_USD_Billion":float(td[2].contents[0][:-1])}
            data_list.append(data_dict)
    df = pd.DataFrame(data_list)
    return df

def transform(df, csv_path):
    data = pd.read_csv(csv_path, usecols=['Currency', 'Rate'])
    exchange_rate = data.set_index(data["Currency"]).to_dict()["Rate"]
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]    
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    query = pd.read_sql(query_statement, sql_connection)
    print(query)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
table_attribs = ["Name", "MC_USD_Billion"]
table_attribs_final = ["Name", "MC_USD_Billion", "MC_GBP_Billion",
                        "MC_EUR_Billion", "MC_INR_Billion"]
output_path = "./Largest_banks_data.csv"
table_name = "Largest_banks"
log_file = "code_log.txt"
db_name = "Banks.db"


log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
#print(df)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, csv_path)
#print(df)
print(df["MC_EUR_Billion"][4])
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
log_progress('Query 1 Process Complete.')
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement2, sql_connection)
log_progress('Query 2 Process Complete.')
query_statement3 = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement3, sql_connection)
log_progress('Query 3 Process Complete.')
sql_connection.close()