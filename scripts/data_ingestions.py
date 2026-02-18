import os
import pandas as pd
import requests
from sqlalchemy import create_engine
import openpyxl

dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')

class DataIngestion:
    def __init__(self, db_url = None):
        self.engine = create_engine(db_url) if db_url else None

    def ingest_csv(self, file_path):
        file_path = os.path.join(dir, file_path)
        try:
            df = pd.read_csv(file_path)
            print(f"Successfully ingested data from {file_path}")
            return df
        except Exception as e:
            print(f"Error ingesting data from {file_path}: {e}")
            return None

    def load_excel(self, file_name, sheet_name=0):
        file_path = os.path.join(dir, file_name)
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name )
            print(f"Successfully ingested data from {file_path}")
            return df
        except Exception as e:
            print(f"Error ingesting data from {file_path}: {e}")
            return None

    def connect_db(self, db_url):
        try:
            self.engine = create_engine(db_url)
            print(f"Successfully connected to database at {db_url}")
        except Exception as e:
            print(f"Error connecting to database at {db_url}: {e}")
            self.engine = None

    def load_from_db(self, query):
        if not self.engine:
            print("Database connection not established.")
            return None
        try:
            df = pd.read_sql(query, self.engine)
            print(f"Successfully loaded data from database with query: {query}")
            return df
        except Exception as e:
            print(f"Error loading data from database with query: {query}: {e}")
            return None            
        
    def fetch_api_data(self, url, params=None):
      try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            print(f"Successfully fetched data from API at {url}")
            return df   #return dataframe
        else:
            print(f"Failed to fetch data from API at {url}. Status code: {response.status_code}")
            return None
      except Exception as e:
         print(f"Error fetching data from API at {url}: {e}")
         return None

