import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import logging
import time
import sqlite3

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)

conn = sqlite3.connect('inventory.db')

def ingest_db(df, table_name, conn):
    """Ingest dataframe into database table"""
    df.to_sql(table_name, con=conn, if_exists="replace", index=False)


def load_raw_data():
    '''this function will load the CSVs as dataframe and ingest into db'''
    start=time.time()
    for file in os.listdir('data'):
       if '.csv' in file:
           df = pd.read_csv(os.path.join("data", file))
           print(f"Reading {file} → shape {df.shape}")
           logging.info(f"Ingesting{file}in db")
           ingest_db(df, file[:-4], conn)
    end=time.time()
    total_time=(end-start)/60
    logging.info('-----Ingestion Complete-------')

    logging.info(f'\nTotal Time Taken: (total_time) minutes')

if __name__=='__main__':
   load_raw_data()