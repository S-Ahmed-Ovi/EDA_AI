import streamlit as st
import requests
import pandas as pd
import json
from io import BytesIO
import sys
import os
import re
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# For AI agent and rule-based cleaning
from scripts.data_cleaning import DataCleaning
from scripts.ai_agent import AIAgent


# Initialize
data_cleaning = DataCleaning()
ai_agent = AIAgent()

# FastAPI backend URL
BACKEND_URL = "http://localhost:8000"

# Streamlit setup
st.set_page_config(page_title="AI Data Cleaning", layout="wide")
st.title("AI-Powered Data Cleaning Application")

# Sidebar: select data source
st.sidebar.header("Select Data Source")
data_source = st.sidebar.selectbox(
    "Choose a data source",
    ["CSV File", "Excel File", "Database", "API"],
    index=0
)


# Helper function to call backend
@st.cache_data(show_spinner=False)
def clean_backend(file=None, db_payload=None, api_payload=None, endpoint="clean-data/"):
    try:
        if file:
            files = {"file": (file.name, file.getvalue())}
            response = requests.post(f"{BACKEND_URL}/{endpoint}", files=files)
        elif db_payload:
            response = requests.post(f"{BACKEND_URL}/clean-db-data/", json=db_payload)
        elif api_payload:
            response = requests.post(f"{BACKEND_URL}/clean-api-data/", json=api_payload)
        else:
            return None

        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Error from backend: {response.text}")
            return None
    except Exception as e:
        st.error(f"Request failed: {e}")
        return None


# CSV / Excel File Upload
if data_source in ["CSV File", "Excel File"]:
    st.subheader(f"Upload a {data_source}")
    uploaded_file = st.file_uploader(f"Upload a {data_source}", type=["csv", "xlsx", "xls"])

    if uploaded_file:
        if uploaded_file.size > 50 * 1024 * 1024:
            st.error("File too large! Please upload a file smaller than 50MB.")
        else:
            st.markdown("### Original Data Preview")
            if data_source == "CSV File":
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            st.dataframe(df.head(10))

            if st.button("Clean Data"):
                # Step 1: Rule-based cleaning
                with st.spinner("Performing rule-based cleaning..."):
                    cleaned_df = data_cleaning.clean_data(df)

                # Step 2: AI cleaning with robust JSON parsing
                batch_size = 5  # smaller batch for testing
                total_batches = (len(cleaned_df) + batch_size - 1) // batch_size
                progress_bar = st.progress(0)
                ai_results = []
                issues_found_all = []
                cleaning_strategy_all = []

                import json, re, ast

                for i, start in enumerate(range(0, len(cleaned_df), batch_size)):
                    batch = cleaned_df.iloc[start:start + batch_size]
                    st.write(f"Processing batch {i + 1} of {total_batches}...")

                    # Call AI agent once
                    ai_response = ai_agent._clean_batch(batch)
                    st.write("Raw AI output for this batch:")
                    st.text(ai_response)

                    # ------------------------------
                    # Robust JSON extraction
                    # ------------------------------
                    import json, ast
                    
                    if isinstance(ai_response, dict):
                         ai_json = ai_response
                    elif isinstance(ai_response, str):
                        try:
                            ai_json = json.loads(ai_response)
                        except json.JSONDecodeError:
                              try:
                                 ai_json = ast.literal_eval(ai_response)
                              except Exception:
                                 st.warning(f"AI output could not be parsed for batch {i+1}")
                                 ai_json = {"cleaned_data": [], "issues_found": [], "cleaning_strategy": []}
                    else:
                        st.warning(f"AI output is neither str nor dict for batch {i+1}")
                        ai_json = {"cleaned_data": [], "issues_found": [], "cleaning_strategy": []}

                    # Extend results
                    ai_results.extend(ai_json.get("cleaned_data", []))
                    issues_found_all.extend(ai_json.get("issues_found", []))
                    cleaning_strategy_all.extend(ai_json.get("cleaning_strategy", []))

                    progress_bar.progress((i + 1) / total_batches)

                # Combine all batches into DataFrame
                cleaned_ai_df = pd.DataFrame(ai_results)

                # Show AI issues & strategy in sidebar
                st.sidebar.subheader("AI Cleaning Issues")
                st.sidebar.write(issues_found_all if issues_found_all else "No issues reported")
                st.sidebar.subheader("Cleaning Strategy")
                st.sidebar.write(cleaning_strategy_all if cleaning_strategy_all else "No strategy reported")

                # Show side-by-side comparison
                if not cleaned_ai_df.empty:
                    st.markdown("### Original vs AI Cleaned Data (first 10 rows)")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("Original Data")
                        st.dataframe(df.head(10))
                    with col2:
                        st.subheader("AI Cleaned Data")
                        st.dataframe(cleaned_ai_df.head(10))
                else:
                    st.error("AI cleaned data is empty.")


# Database Query
elif data_source == "Database":
    st.subheader("Enter Database Connection Details")
    db_url = st.text_input("Database URL (e.g., postgresql://user:pass@host:port/dbname)")
    query = st.text_area("SQL Query to fetch data")

    if st.button("Clean Data"):
        if not db_url or not query:
            st.error("Database URL and Query cannot be empty!")
        else:
            db_payload = {"db_url": db_url, "query": query}
            with st.spinner("Fetching and cleaning data..."):
                result = clean_backend(db_payload=db_payload)
                if result:
                    st.sidebar.subheader("AI Cleaning Issues")
                    st.sidebar.write(result.get("issues_found", []))
                    st.sidebar.subheader("Cleaning Strategy")
                    st.sidebar.write(result.get("cleaning_strategy", []))

                    cleaned_df = pd.DataFrame(result.get("cleaned_data", []))
                    if not cleaned_df.empty:
                        st.markdown("### Original vs Cleaned Data")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.subheader("Original Data")
                            try:
                                df = pd.read_sql(query, db_url)
                                st.dataframe(df.head(10))
                            except:
                                st.write("Could not fetch original data")
                        with col2:
                            st.subheader("Cleaned Data")
                            st.dataframe(cleaned_df.head(10))
                    else:
                        st.error("Cleaned data is empty.")


# API Data Input
elif data_source == "API":
    st.subheader("Enter API Details")
    api_url = st.text_input("API URL")
    params_input = st.text_area("Query Parameters (JSON format)")

    if st.button("Clean Data"):
        if not api_url:
            st.error("API URL cannot be empty!")
        else:
            # Validate JSON
            try:
                params = json.loads(params_input) if params_input else {}
            except json.JSONDecodeError:
                st.error("Invalid JSON in parameters. Using empty parameters.")
                params = {}

            api_payload = {"api_url": api_url, "params": params}

            with st.spinner("Fetching and cleaning data..."):
                result = clean_backend(api_payload=api_payload)
                if result:
                    st.sidebar.subheader("AI Cleaning Issues")
                    st.sidebar.write(result.get("issues_found", []))
                    st.sidebar.subheader("Cleaning Strategy")
                    st.sidebar.write(result.get("cleaning_strategy", []))

                    cleaned_df = pd.DataFrame(result.get("cleaned_data", []))
                    if not cleaned_df.empty:
                        st.markdown("### Original vs Cleaned Data (first 10 rows)")
                        col1, col2 = st.columns(2)
                        # Fetch original API data
                        try:
                            response = requests.get(api_url, params=params)
                            response.raise_for_status()
                            df = pd.DataFrame(response.json())
                        except:
                            df = pd.DataFrame()
                        with col1:
                            st.subheader("Original Data")
                            st.dataframe(df.head(10))
                        with col2:
                            st.subheader("Cleaned Data")
                            st.dataframe(cleaned_df.head(10))
                    else:
                        st.error("Cleaned data is empty.")