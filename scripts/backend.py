import sys
import os
import pandas as pd
import io
import aiohttp
from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from sqlalchemy import create_engine
from pydantic import BaseModel
import requests

# Ensure the scripts folder is in the system path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from scripts.ai_agent import AIAgent # Import the AIAgent class
from scripts.data_cleaning import DataCleaning # Import the DataCleaning class

app = FastAPI()

#Initialize the AI Agent and Data Cleaning instances
ai_agent = AIAgent()
data_cleaning = DataCleaning()

#Endpoints

#Endpoint for CSV and Excel

@app.post("/clean-data/")
async def clean_data(file: UploadFile = File(...), missing_value_strategy: str = Query('mean'), outlier_column: str = Query(None), irrelevant_columns: str = Query(None), categorical_column: str = Query(None), data_type_fixes: str = Query(None)):
    try:
        #Read the uploaded file into a DataFrame
        contents = await file.read()
        file_extension = os.path.splitext(file.filename)[1].lower()
        
        #Load the data into a DataFrame based on the file extension
        if file_extension == '.csv':
            df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        elif file_extension in ['.xlsx', '.xls']:
            df = pd.read_excel(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Please upload a CSV or Excel file.") 

        #step 1: Clean the data using the DataCleaning class
        cleaned_df = data_cleaning.clean_data(df, missing_value_strategy, outlier_column, irrelevant_columns.split(',') if irrelevant_columns else None, categorical_column, eval(data_type_fixes) if data_type_fixes else None)       

        #step 2: AI Agent Cleaning
        ai_cleaned_df = ai_agent.clean_data(cleaned_df)

        #Ensure AI Output is in DataFrame format
        if isinstance(ai_cleaned_df, str):
            from io import StringIO
            ai_cleaned_df = pd.read_csv(StringIO(ai_cleaned_df))

        return{"cleaned_data": ai_cleaned_df.to_dict(orient='records')}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

#Endpoint for Database Connection
class DBQuery(BaseModel):
    db_url: str
    query: str

@app.post("/clean-db-data/")
async def clean_db_data(db_query: DBQuery):
    try:
        engine = create_engine(db_query.db_url)
        df = pd.read_sql(db_query.query, engine)

        #step 1: Rule based cleaning
        cleaned_df = data_cleaning.clean_data(df)

        #step 2: AI Agent Cleaning
        ai_cleaned_df = ai_agent.clean_data(cleaned_df)

        #Ensure AI Output is in DataFrame format
        if isinstance(ai_cleaned_df, str):
            from io import StringIO
            ai_cleaned_df = pd.read_csv(StringIO(ai_cleaned_df))

        return{"cleaned_data": ai_cleaned_df.to_dict(orient='records')}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

#Endpoint for API Data Cleaning
class APIDataRequest(BaseModel):
    api_url: str
    params: dict = None

@app.post("/clean-api-data/")
async def clean_api_data(api_data_request: APIDataRequest):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_data_request.api_url, params=api_data_request.params) as response:
                if response.status != 200:
                    raise HTTPException(status_code=response.status, detail=f"API request failed with status code {response.status}")
                data = await response.json()
                df = pd.DataFrame(data)

        #step 1: Rule based cleaning
        cleaned_df = data_cleaning.clean_data(df)

        #step 2: AI Agent Cleaning
        ai_cleaned_df = ai_agent.clean_data(cleaned_df)

        #Ensure AI Output is in DataFrame format
        if isinstance(ai_cleaned_df, str):
            from io import StringIO
            ai_cleaned_df = pd.read_csv(StringIO(ai_cleaned_df))

        return{"cleaned_data": ai_cleaned_df.to_dict(orient='records')}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#Run Server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)