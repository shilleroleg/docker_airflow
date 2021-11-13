# Idea:  dmitry-brazhenko / airflow_tutorial
# Change: shilleroleg@gmail.com
import os

import pandas as pd
import numpy as np
import psycopg2
import sqlalchemy as sa

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

first_dag = DAG(
    "first_dag",
    description='Python DAG example',
    schedule_interval="*/5 * * * *",    # every 5 minutes
    # schedule_interval="* * * * *",    # every minutes
    start_date=days_ago(0, 0, 0, 0, 0),
    tags=['python'],
    doc_md='*Python DAG doc* :)'
)

def get_engine():
    user = 'postgres'
    password = '123'
    host = 'host.docker.internal'
    dbname = 'postgres'
    engine = sa.create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{dbname}')
    return engine


def save_db(query, file_name):
    df_db = pd.read_sql(query, get_engine())
    print(df_db.head(2))
    df_db.to_csv(file_name, index=False)


def merge_df():
    df_prod = pd.read_csv('products.csv')
    df_user = pd.read_csv('users.csv')
    df_sum = df_user.merge(df_prod, how='left', left_on='id_products', right_on='id_products')
    print(df_sum.head(2))
    df_sum.to_csv('sum.csv', index=False)


def pivot_df():
    df_sum=pd.read_csv('sum.csv')
    df_pivot = df_sum.pivot_table(index='sex', values=['age', 'income'], aggfunc=np.mean)
    print(df_pivot.head(2))
    df_pivot.to_csv('pivot_users.csv')

# change working directory to /
# You should not use it in production

os.chdir("/")

merge_dataframe = PythonOperator(
    task_id='merge_df',
    python_callable=merge_df,
    dag=first_dag
)

pivot_dataframe = PythonOperator(
    task_id='pivot_df',
    python_callable=pivot_df,
    dag=first_dag
)

merge_dataframe >> pivot_dataframe

query_list = ['SELECT * FROM airflow.products', 
              'SELECT * FROM airflow.users']
name_list = ['products.csv',
             'users.csv']

for query, file_name in zip(query_list, name_list):
    download_dataframe = PythonOperator(
    task_id='save_' + file_name[:-4],
    python_callable=save_db,
    op_kwargs={'query': query, 
               'file_name': file_name},
    dag=first_dag
    )

    download_dataframe >> merge_dataframe




