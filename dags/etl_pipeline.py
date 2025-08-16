from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def etl():
    import pandas as pd
    orders = pd.read_csv('/opt/airflow/data/raw/orders.csv')
    od = pd.read_csv('/opt/airflow/data/raw/order_details.csv')
    # merge and clean...
    df = pd.merge(orders, od, on='order_id')
    df.to_sql('ecom_complete', con='sqlite:///data.db', if_exists='replace')

with DAG('ecom_etl', start_date=datetime(2025,8,16), schedule_interval=None) as dag:
    task = PythonOperator(
        task_id='run_etl',
        python_callable=etl
    )
