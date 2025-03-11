
from flask import Flask, render_template, request, jsonify
import os
import pandas as pd
import json
import requests
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Use environment variables for configuration
AMC_API_ENDPOINT = os.environ.get('AMC_API_ENDPOINT')
AMC_CLIENT_ID = os.environ.get('AMC_CLIENT_ID')
AMC_CLIENT_SECRET = os.environ.get('AMC_CLIENT_SECRET')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA')

@app.route('/')
def index():
    """Render the main page of the application."""
    return render_template('index.html')

@app.route('/run-etl', methods=['POST'])
def run_etl():
    """API endpoint to trigger the ETL process."""
    try:
        # Get query parameters from request
        data = request.json
        query_name = data.get('query_name')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # Validate input
        if not query_name:
            return jsonify({"error": "Query name is required"}), 400
        
        # Execute ETL process
        result = execute_etl_process(query_name, start_date, end_date)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        return jsonify({"error": str(e)}), 500

def execute_etl_process(query_name, start_date=None, end_date=None):
    """
    Execute the ETL process for Amazon Marketing Cloud data.
    
    Parameters:
    - query_name: Name of the predefined query to run
    - start_date: Optional start date filter (YYYY-MM-DD)
    - end_date: Optional end date filter (YYYY-MM-DD)
    
    Returns:
    - Dictionary with ETL results
    """
    logger.info(f"Starting ETL process for query: {query_name}")
    
    # Step 1: Extract - Get data from Amazon Marketing Cloud
    raw_data = extract_data_from_amc(query_name, start_date, end_date)
    
    # Step 2: Transform - Process the data
    transformed_data = transform_amc_data(raw_data)
    
    # Step 3: Load - Load data to Snowflake
    load_result = load_data_to_snowflake(transformed_data, query_name)
    
    return {
        "status": "success",
        "records_processed": len(transformed_data),
        "timestamp": datetime.now().isoformat(),
        "query_name": query_name,
        "load_details": load_result
    }

def extract_data_from_amc(query_name, start_date=None, end_date=None):
    """
    Extract data from Amazon Marketing Cloud API.
    
    Parameters:
    - query_name: Name of the predefined query to run
    - start_date: Optional start date filter
    - end_date: Optional end date filter
    
    Returns:
    - Raw data from AMC
    """
    logger.info(f"Extracting data from AMC for query: {query_name}")
    
    # Load predefined SQL queries
    query_file_path = 'queries/amc_queries.json'
    with open(query_file_path, 'r') as f:
        query_definitions = json.load(f)
    
    # Get the SQL query
    if query_name not in query_definitions:
        raise ValueError(f"Query '{query_name}' not found in query definitions")
    
    query_sql = query_definitions[query_name]['sql']
    
    # Replace date placeholders if provided
    if start_date:
        query_sql = query_sql.replace('{{start_date}}', start_date)
    if end_date:
        query_sql = query_sql.replace('{{end_date}}', end_date)
    
    # Get authentication token
    auth_token = get_amc_auth_token()
    
    # Make API request to AMC
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'query': query_sql,
        'format': 'CSV'  # Request CSV format for easier pandas processing
    }
    
    response = requests.post(AMC_API_ENDPOINT, headers=headers, json=payload)
    
    if response.status_code != 200:
        logger.error(f"AMC API error: {response.text}")
        raise Exception(f"Failed to extract data from AMC: {response.text}")
    
    # Parse the CSV response
    csv_data = response.text
    return csv_data

def get_amc_auth_token():
    """Get authentication token for AMC API."""
    # Implementation depends on AMC authentication method
    # This is a placeholder - you would implement the actual auth flow
    logger.info("Getting AMC authentication token")
    
    # Example OAuth2 client credentials flow
    auth_url = f"{AMC_API_ENDPOINT.split('/api')[0]}/auth/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': AMC_CLIENT_ID,
        'client_secret': AMC_CLIENT_SECRET
    }
    
    response = requests.post(auth_url, data=payload)
    
    if response.status_code != 200:
        logger.error(f"Authentication error: {response.text}")
        raise Exception("Failed to authenticate with AMC")
    
    return response.json()['access_token']

def transform_amc_data(raw_data):
    """
    Transform the raw data from AMC.
    
    Parameters:
    - raw_data: CSV string from AMC
    
    Returns:
    - Transformed pandas DataFrame
    """
    logger.info("Transforming AMC data")
    
    # Parse CSV into pandas DataFrame
    df = pd.read_csv(pd.StringIO(raw_data))
    
    # Apply transformations:
    # 1. Convert date columns to datetime
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col])
    
    # 2. Handle missing values
    df = df.fillna(0)  # Replace NaN with zeros for numeric columns
    
    # 3. Additional transformations can be added here based on specific needs
    
    return df

def load_data_to_snowflake(df, table_name):
    """
    Load transformed data to Snowflake.
    
    Parameters:
    - df: Transformed pandas DataFrame
    - table_name: Target table name in Snowflake
    
    Returns:
    - Dictionary with load results
    """
    logger.info(f"Loading data to Snowflake table: {table_name}")
    
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
        
        # Create Snowflake connection
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        # Load data to Snowflake
        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            auto_create_table=True
        )
        
        conn.close()
        
        return {
            "success": success,
            "chunks_processed": num_chunks,
            "rows_loaded": num_rows
        }
    
    except Exception as e:
        logger.error(f"Snowflake loading error: {str(e)}")
        raise Exception(f"Failed to load data to Snowflake: {str(e)}")

if __name__ == '__main__':
    # Create queries directory if it doesn't exist
    os.makedirs('queries', exist_ok=True)
    
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    app.run(host='0.0.0.0', port=8080, debug=True)