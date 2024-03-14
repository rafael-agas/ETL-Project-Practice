from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    rows = soup.find_all("tbody")[2].find_all("tr")
    data_list = []
    for row in rows:
        td = row.find_all("td")
        if len(td) != 0:
            if td[0].find("a") is not None and td[2].contents[0] != '—':
                data_dict = {"Countries": td[0].a.contents[0],
                         "GDP_USD_millions": td[2].contents[0]}
                data_list.append(data_dict)
    df = pd.DataFrame(data_list)
    return df

def transform(df):
    money = df["GDP_USD_millions"].tolist()
    money = [float("".join(item.split(','))) for item in money]
    money = [np.round(item/1000,2) for item in money]
    df["GDP_USD_millions"] = money
    df = df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    query = pd.read_sql(query_statement, sql_connection)
    print(query)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

log_file = "etl_project_log.txt" 
csv_path = "./Countries_by_GDP.csv"
db_name = "WOrld_Economies.db"
table_name = 'Countries_by_GDP'
table_attribs = ['Country','GDP_USD_millions']
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()