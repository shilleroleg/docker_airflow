# Author:  dmitry-brazhenko / airflow_tutorial
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
    schedule_interval="* * * * *",
    start_date=days_ago(0, 0, 0, 0, 0),
    tags=['python'],
    doc_md='*Python DAG doc* :)'
)

def get_engine():
    engine = sa.create_engine('postgresql+psycopg2://postgres:123@host.docker.internal/postgres')
    return engine


def save_emp():
    query_emp = 'SELECT * FROM vr_startup.employees'
    df_emp = pd.read_sql(query_emp, get_engine())
    print(df_emp.head(2))
    df_emp.to_csv('employees.csv', index=False)


def save_project():
    query_proj = 'SELECT * FROM vr_startup.projects'
    df_proj = pd.read_sql(query_proj, get_engine())
    print(df_proj.head(2))
    df_proj.to_csv('project.csv', index=False)


def merge_df():
    df_emp = pd.read_csv('employees.csv')
    df_proj = pd.read_csv('project.csv')
    df_sum = df_emp.merge(df_proj, how='left', left_on='current_project', right_on='project_id')
    print(df_sum.head(2))
    df_sum.to_csv('sum.csv', index=False)


def pivot_df():
    df_sum=pd.read_csv('sum.csv')
    df_pivot = df_sum.pivot_table(index='position', values=['start_date', 'end_date'], aggfunc=np.max)
    print(df_pivot.head(2))
    df_pivot.to_csv('pivot_person.csv')


download_dataframe_employees = PythonOperator(
    task_id='save_emp',
    python_callable=save_emp,
    dag=first_dag
)

download_dataframe_projects = PythonOperator(
    task_id='save_project',
    python_callable=save_project,
    dag=first_dag
)

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


# change working directory to /
# You should not use it in production

os.chdir("/")

download_dataframe_employees >> merge_dataframe
download_dataframe_projects >> merge_dataframe
merge_dataframe >> pivot_dataframe
