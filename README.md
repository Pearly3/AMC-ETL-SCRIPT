# Amazon Marketing Cloud (AMC) ETL Application

## Overview

This application provides an ETL (Extract, Transform, Load) solution for Amazon Marketing Cloud data. It extracts campaign and audience data from the AMC API, performs transformations, and loads the results into a Snowflake data warehouse. This tool is designed to be used by marketing analysts and data engineers working with Amazon Advertising data.

## Key Features

- **Extract**: Connect to Amazon Marketing Cloud API and extract data using predefined SQL queries
- **Transform**: Process and clean the data for analysis
- **Load**: Store the results in Snowflake for further analysis and reporting
- **Web Interface**: Simple web interface to trigger ETL jobs with date parameters
- **Secure**: Uses environment variables for sensitive credentials

## System Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Flask Web   │     │ ETL Engine  │     │ Amazon      │     │ Snowflake   │
│ Interface   │────>│ (Python/    │────>│ Marketing   │────>│ Data        │
│             │     │  Pandas)    │     │ Cloud API   │     │ Warehouse   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

## Prerequisites

- Amazon Marketing Cloud API credentials (client ID and secret)
- Snowflake account with appropriate permissions
- Python 3.8 or higher

## Installation

1. Clone this repository
2. Set up environment variables:

```
AMC_API_ENDPOINT=https://advertising-api.amazon.com/amc/...
AMC_CLIENT_ID=your_client_id
AMC_CLIENT_SECRET=your_client_secret
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_snowflake_account
SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
SNOWFLAKE_DATABASE=your_snowflake_database
SNOWFLAKE_SCHEMA=your_snowflake_schema
```

3. Install the required packages:
```
pip install flask pandas requests snowflake-connector-python
```

4. Run the application:
```
python app.py
```

## Usage

1. Access the web interface at `http://localhost:8080`
2. Select an AMC query type from the dropdown
3. Specify date range parameters
4. Click "Run ETL Process"
5. View the results displayed on the page

## AMC Queries

The application includes several predefined AMC queries:

1. **Audience Overlap**: Analyze audience overlap between different campaigns
2. **Conversion Path**: Track user conversion paths across touchpoints
3. **Campaign Performance**: Analyze key performance metrics for campaigns
4. **Frequency Distribution**: Understand impression frequency distribution

You can add custom queries by editing the `queries/amc_queries.json` file.

## Data Flow

1. **Extract**: The application connects to the AMC API using OAuth2 authentication and retrieves data using SQL queries
2. **Transform**: The raw data is processed using pandas:
   - Date columns are converted to datetime format
   - Missing values are handled
   - Additional transformations as needed
3. **Load**: The transformed data is loaded into Snowflake tables

## Security Considerations

- All sensitive credentials are stored as environment variables
- API tokens are not persisted
- HTTPS is used for API communication

## Extending the Application

You can extend this application by:

1. Adding new AMC queries to the `queries/amc_queries.json` file
2. Implementing additional transformations in the `transform_amc_data()` function
3. Creating reporting dashboards that connect to the Snowflake database

## Troubleshooting

Common issues:

1. **Authentication errors**: Verify your AMC API credentials
2. **Snowflake connection errors**: Check network connectivity and credentials
3. **Missing data**: Verify date ranges and query parameters