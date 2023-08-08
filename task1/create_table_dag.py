from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id='webhook_create_table',
    start_date=datetime(2023, 8, 7),
    schedule="@once",
) as dag:
    task_create_table = PostgresOperator(
        task_id='create_table_headers',
        postgres_conn_id='postgres_webhook',
        sql=f""" 
                CREATE TABLE IF NOT EXISTS headers_webhook (
                    Server CHAR(10),
                    Content_Type VARCHAR (50),
                    Transfer_Encoding VARCHAR (50),
                    Vary VARCHAR (50),
                    Request_Id VARCHAR (50) NOT NULL,
                    Token_Id VARCHAR (50) NOT NULL,
                    Cache_Control VARCHAR (50),
                    Date date NOT NULL,
                    Content_Encoding VARCHAR (30),
                    created_on TIMESTAMP NOT null default CURRENT_TIMESTAMP,
                    PRIMARY KEY (Request_Id) );"""
        )

    task_create_table