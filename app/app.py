import streamlit as st
import requests
import pandas as pd
import json
from io import StringIO

#FastAPI backend URL
BACKEND_URL = "http://localhost:8000"

#Streamlit UI configuration
st.title("AI-Powered Data Cleaning Application")
st.set_page_config(page_title="AI Data Cleaning", layout="wide")

#Sidebar for data source selection
st.sidebar.header("Select Data Source")
data_source = st.sidebar.selectbox("Choose a data source", ["CSV File", "Excel File", "Database", "API"], index=0)

#Main Title
st.markdown("## Upload your dataset and let the AI clean it for you!")

# CSV/Excel File Upload
if data_source in ["CSV File", "Excel File"]:
    st.subheader(f"Upload a file")
    uploaded_file = st.file_uploader(f"Upload a {data_source}", type=["csv", "xlsx", "xls"])
    if uploaded_file is not None:
        st.markdown("### Original Data")
        if data_source == "CSV File":
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        st.dataframe(df.head(10))

        if st.button("Clean Data"):
            with st.spinner("Cleaning data..."):
                files = {"file": (uploaded_file.name, uploaded_file.getvalue())}
                response = requests.post(f"{BACKEND_URL}/clean-data/", files=files)
                if response.status_code == 200:
                    cleaned_data = response.json().get("cleaned_data", [])
                    cleaned_df = pd.DataFrame(cleaned_data)
                    st.markdown("### Cleaned Data")
                    st.dataframe(cleaned_df.head(10))
                else:
                    st.error(f"Error cleaning data: {response.text}")

# Database query input
elif data_source == "Database":
    st.subheader("Enter Database Connection Details")
    db_url = st.text_input("Database URL (e.g., postgresql://user:password@host:port/dbname)")
    query = st.text_area("SQL Query to fetch data")

    if st.button("Clean Data"):
        with st.spinner("Cleaning data..."):
            payload = {"db_url": db_url, "query": query}
            response = requests.post(f"{BACKEND_URL}/clean-db-data/", json=payload)
            if response.status_code == 200:
                cleaned_data = response.json().get("cleaned_data", [])
                cleaned_df = pd.DataFrame(cleaned_data)
                st.markdown("### Cleaned Data")
                st.dataframe(cleaned_df.head(10))
            else:
                st.error(f"Error cleaning data: {response.text}")

# API data input
elif data_source == "API":
    st.subheader("Enter API Details")
    api_url = st.text_input("API URL")
    params_input = st.text_area("Query Parameters (JSON format)")

    if st.button("Clean Data"):
        with st.spinner("Cleaning data..."):
            params = json.loads(params_input) if params_input else {}
            payload = {"api_url": api_url, "params": params}
            response = requests.post(f"{BACKEND_URL}/clean-api-data/", json=payload)
            if response.status_code == 200:
                cleaned_data = response.json().get("cleaned_data", [])
                cleaned_df = pd.DataFrame(cleaned_data)
                st.markdown("### Cleaned Data")
                st.dataframe(cleaned_df.head(10))
            else:
                st.error(f"Error cleaning data: {response.text}")