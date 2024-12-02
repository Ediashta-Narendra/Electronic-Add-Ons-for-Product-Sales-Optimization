"""
=================================================
DAG for Electronic Add-Ons for Product Sales Optimization

Created by  : Ediashta Narendra

Program ini dibuat dengan tujuan automatisasi fetch data dari PostgreSQL, clean data menggunakan python dan export data ke ElasticSearch.
Dataset yang digunakan adalah datset transaksi produk elektronik pada rentang waktu tahun 2023 - 2024 dengan ukuran data 20,000 baris. 
Sebagai informasi tambahan, dataset yang digunakan juga terlampir dalam derektori ini, dengan nama electronic_data_raw.csv 
dan akan dihasilkan data yang sudah diproses dengan nama electronic_data_clean.csv. 
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
Fungsi/node ini ditujukan untuk memperoleh dataset raw yang tersimpan pada PostgresSQL, 
sehingga dapat dilakukan pemrosesan data lebih lanjut berupa data cleaning.
Berikut ini adalah ketergangan detail fungsi fetch_data:

Parameters:
    host        : string - hostname pada PostgresSQL
    database    : string - nama database pada PostgresSQL yang menjadi lokasi database penyimpanan .csv raw 
    user        : string - nama user pada PostgresSQL yang memiliki hak akses database dan table didalamnya
    password    : string - credential dari nama user yang digunakan
    port        : integer - port number yang digunakan untuk melakukan koneksi dengan PostgresSQL

Returns:
    df          : DataFrame - Data yang diambil dari PostgreSQL dalam bentuk pandas DataFrame.

Contoh Penggunaan:
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
    Fungsi/node ini ditujukan untuk proses cleaning data dengan menghapus duplikat, handling misiing, expand kolom add-ons purchased,
    dan memperbaiki format dari beberapa kolom. Output yang dihasilkan adalah menyimpan data yang sudah dibersihkan ke CSV.

    Parameters:
        csv_path    : string - Lokasi penyimpanan file csv untuk data yang akan dibersihkan dan nantinya menjadi tempat penyimpanan csv clean.

    Returns:
        df_clean    : DataFrame - Data yang sudah dibersihkan akan disimpan kembali ke file CSV.
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
    Node/function ini ditujukan untuk mengekspor data yang sudah dibersihkan sebelumnya ke indeks Elasticsearch.

    Parameters:
        csv_path        : string - Lokasi penyimpanan file CSV yang berisi data yang sudah dibersihkan.
        elastic_host    : string - URL host Elasticsearch (default: "http://elasticsearch:9200").
        index_name      : string - Nama indeks di Elasticsearch (default: "electo_trans_data").

    Returns:
        None            : Data akan dikirim ke Elasticsearch sebagai dokumen.

    Contoh Pengunaan:
        csv_path = "/path/to/your/clean_dataset.csv"  # Ganti dengan lokasi file csv yang dituju
        elastic_host = "http://elasticsearch:9200"
        index_name = "electo_trans_data"
        export_to_elastic(csv_path=csv_path, elastic_host=elastic_host, index_name=index_name)

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
Pada pendefinisian schedule inteval pipeline digunakan Cron Expression dengan keterangan sebagai berikut:
    Penjabaran of '30 6 * * *':
        30 - Run at minute 30
        6 - Run at hour 6 (6:30 AM)
        * - Run every day of the month
        * - Run every month
        * - Run on every day of the week
    Sehingga definisi schedule_interbal = '30 6 * * *' dibawah ini menunjukkan bahwa task akan di-run setiap hari pada pukul 6.30 AM.
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