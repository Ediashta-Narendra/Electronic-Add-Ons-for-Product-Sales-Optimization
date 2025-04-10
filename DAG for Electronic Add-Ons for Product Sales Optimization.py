"""
=================================================
DAG for Electronic Add-Ons for Product Sales Optimization

Created by  : Ediashta Narendra - ver1.1 10 Apr 2025

This program was developed to automate the workflow of fetching data from PostgreSQL, 
cleaning the data using Python, and exporting the processed data to Elasticsearch.

The dataset used is a transaction record of electronic add-on products, 
spanning the years 2023 to 2024 and consisting of approximately 20,000 rows.

For additional reference, the raw dataset is included in this directory as 
'electronic_data_raw.csv', and the cleaned, processed version generated through 
the ETL pipeline is saved as 'electronic_data_clean.csv'.
=================================================
"""

#import libraries
import pandas as pd 
import psycopg2 
from elasticsearch import Elasticsearch 
from airflow import DAG 
from airflow.operators.python import PythonOperator 
import datetime as dt
from datetime import timedelta

# Define output Path
csv_path = "/opt/airflow/dags/P2M3_ediashta_narendra_data_clean.csv"

# TASK 1 FETCH DATA FROM POSTGRES
def fetch_data():
"""
    This function/node is intended to retrieve the raw dataset stored in PostgreSQL, 
    enabling further data processing such as data cleaning.

    Parameters:
        host        : str   - The hostname of the PostgreSQL server.
        database    : str   - The name of the PostgreSQL database where the raw CSV data is stored.
        user        : str   - The username with access privileges to the database and its tables.
        password    : str   - The corresponding credential for the specified user.
        port        : int   - The port number used to establish the connection with PostgreSQL.

    Returns:
        df          : DataFrame - The data retrieved from PostgreSQL, returned as a pandas DataFrame.

    Example Usage:
        df = fetch_data(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432)
"""
    # Connect to Postgre
    conn = psycopg2.connect(
        host="postgres",        
        database="airflow",     
        user="airflow",         
        password="airflow",     
        port=5432)
    #define SQL query
    query = "SELECT * FROM table_m3" 
    df = pd.read_sql(query, conn)
    #close connection
    conn.close()
    # Save file to csv path
    df.to_csv(csv_path, index=False)

# TASK 2 DATA CLEANING
def clean_data():
"""
    This function/node is designed for the data cleaning process, which includes 
    removing duplicates, handling missing values, expanding the 'add-ons purchased' column, 
    and correcting data formats for several columns. 

    The final output is a cleaned dataset saved to a CSV file.

    Parameters:
        csv_path    : string - The file path of the CSV file to be cleaned, which will also be the destination for the cleaned CSV.

    Returns:
        df_clean    : DataFrame - The cleaned dataset that will be saved back to a CSV file.

    Example Usage:
        df_clean = clean_data(csv_path='/opt/airflow/dags/electronic_data_raw.csv')
"""
    # Load data
    df = pd.read_csv(csv_path)
   
    # 1. Remove duplicates
    df.drop_duplicates(inplace=True)
    
    # 2. Add "Transaction ID" column
    df['Purchase Date'] = pd.to_datetime(df['Purchase Date'])
    df = df.sort_values(by=['Purchase Date']).reset_index(drop=True)
    df['Transaction ID'] = (
        "TRS" + df['Purchase Date'].dt.strftime('%d%m%y') +
        df.groupby(df['Purchase Date'].dt.date).cumcount().add(1).astype(str).str.zfill(4))
    df.insert(0, 'Transaction ID', df.pop('Transaction ID'))  # Move 'Transaction ID' to the first column
   
    # 3. Handle missing values in "Add-ons Purchased" by filling with "None"
    df['Add-ons Purchased'].fillna('None', inplace=True)
   
    # 4. Expand "Add-ons Purchased" column
    max_addons = df['Add-ons Purchased'].str.split(',').apply(len).max()
    addon_columns = [f'add_on_{i+1}' for i in range(max_addons)]
    df[addon_columns] = df['Add-ons Purchased'].str.split(',', expand=True)
   
    # 5. Drop "Add-ons Purchased" column after expansion
    df.drop(columns=['Add-ons Purchased'], inplace=True)
    
    # 6. Fill the null values in "add_on_(i+1)" columns with 'None'
    df[addon_columns] = df[addon_columns].fillna('None')
      # Remove leading and trailing whitespace from add-on columns
    for col in addon_columns:
        df[col] = df[col].str.strip()

    # 7. Handle missing values for "Gender" based on "Customer ID"
    df['Gender'] = df.groupby('Customer ID')['Gender'].transform(
        lambda x: x.fillna(x.mode()[0]) if not x.mode().empty else x.fillna(df['Gender'].mode()[0]))
    
    # 8. Correct "Paypal" to "PayPal" in "payment_methods" column
    df['Payment Method'] = df['Payment Method'].replace("Paypal", "PayPal")

    # 9. Convert "Purchase Date" to datetime
    df['Purchase Date'] = pd.to_datetime(df['Purchase Date'])

    # 10. Rename columns to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Save to csv
    df.to_csv(csv_path, index=False)

# EXPORT TO ELASTICSEARCH
def export_to_elastic():
"""
    This function/node is intended to export the cleaned dataset to an Elasticsearch index.

    Parameters:
        csv_path        : string - Path to the cleaned CSV file to be exported.
        elastic_host    : string - Elasticsearch host URL (default: "http://elasticsearch:9200").
        index_name      : string - Name of the target index in Elasticsearch (default: "electo_trans_data").

    Returns:
        None            : The data will be sent to Elasticsearch as documents.

    Example Usage:
        csv_path = "/path/to/your/clean_dataset.csv"  # Replace with your actual CSV file path
        elastic_host = "http://elasticsearch:9200"
        index_name = "electo_trans_data"

        export_to_elastic(
            csv_path=csv_path, 
            elastic_host=elastic_host, 
            index_name=index_name
        )
"""
    # Connect to elasticsearch
    elastic = Elasticsearch("http://elasticsearch:9200") 
    df=pd.read_csv(csv_path)
    index_name = "electo_trans_data"
    if not elastic.indices.exists(index=index_name):
        elastic.indices.create(index=index_name)
    #define key values to avoid duplicated in every run
    for i,row in df.iterrows():
        doc=row.to_json()
        unique_id = row['transaction_id'] 
        elastic.index(index=index_name, id=unique_id, body=doc)


# DAG CONFIGURATION
#define deafult argument
default_args= {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 11, 11)}
#define pipeline step 
"""
In the pipeline schedule definition, a Cron Expression is used with the following explanation:
    Breakdown of '30 6 * * *':
        30 - Run at minute 30
        6  - Run at hour 6 (6:30 AM)
        *  - Run every day of the month
        *  - Run every month
        *  - Run on every day of the week
    Therefore, the definition schedule_interval = '30 6 * * *' indicates that the task will be executed every day at 6:30 AM.
"""
with DAG(
        "PipelineM3",
        schedule_interval='30 6 * * *', 
        default_args=default_args, 
        catchup=False) as dag:

        # Node1: Fetch data from PostgreSQL
        fetching_from_postgressql = PythonOperator(
                                task_id='fetch_from_postgres', 
                                python_callable=fetch_data 
                                )
        
        # Node2: Clean data
        cleaning_data = PythonOperator(
                             task_id='clean_data', 
                             python_callable=clean_data
                            )

        # Node3: Export data to Elasticsearch
        exporting_to_elastic = PythonOperator(
                             task_id='export_to_elastic', 
                             python_callable=export_to_elastic
                            )
        fetching_from_postgressql >> cleaning_data >> exporting_to_elastic