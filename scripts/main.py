from data_ingestions import DataIngestion
from data_cleaning import DataCleaning
from ai_agent import AIAgent
import pandas as pd
import numpy as np

# Database configuration
DB_HOST = "localhost"
DB_PORT = "5432"    
DB_NAME = "demodb"
DB_USER = "postgres"
DB_PASSWORD = "    "

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Initialize components
data_ingestion = DataIngestion()
data_cleaning = DataCleaning()
ai_agent = AIAgent()

# Load and clean data from CSV
df_csv = data_ingestion.ingest_csv("../data/sale.csv")
if df_csv is not None:
    print("Data loaded from CSV:", df_csv.head(5))
    df_csv_cleaned = data_cleaning.clean_data(df_csv)
    df_csv_cleaned = ai_agent.clean_data(df_csv_cleaned)
    print("AI Cleaned Data from CSV: ", df_csv_cleaned.head(5))

# Load and clean data from Excel
df_excel = data_ingestion.load_excel("../data/country-code.xlsx")
if df_excel is not None:
    print("Data loaded from Excel:", df_excel.head(5))
    df_excel_cleaned = data_cleaning.clean_data(df_excel)
    df_excel_cleaned = ai_agent.clean_data(df_excel_cleaned)
    print("AI Cleaned Data from Excel: ", df_excel_cleaned.head(5))

# Load and clean data from Database
df_db = data_ingestion.load_from_db("SELECT * FROM sample_table")
if df_db is not None:
    print("Data loaded from Database:", df_db.head(5))
    df_db_cleaned = data_cleaning.clean_data(df_db)
    df_db_cleaned = ai_agent.clean_data(df_db_cleaned)
    print("AI Cleaned Data from Database: ", df_db_cleaned.head(5))   


#Fetch and clean data from API

#Fetch Api data
api_url = "https://jsonplaceholder.typicode.com/posts"
df_api = data_ingestion.fetch_api_data(api_url)

if df_api is not None:
    print("Data loaded from API:", df_api.head())

    if "body" in df_api.columns:
     df_api["body"] = df_api["body"].apply(
        lambda x: x[:100] + "..." if isinstance(x, str) and len(x) > 100 else x
    )


    # Now pass it to your cleaning function (adjusted for list of dicts)
    df_api_cleaned = data_cleaning.clean_data(df_api)
    df_api_cleaned = ai_agent.clean_data(df_api_cleaned)

    # Print first 5 items
    print("AI Cleaned Data from API:", df_api_cleaned.head()) 
    