"""
Created on Wed Jan 19 12:23:21 2022

@author: 19551870
"""
import os
import uuid
import codecs
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from airflow.hooks.base_hook import BaseHook

parameters = {'schema_name': 'pkap_247_sch',
              'table_name': 'call_detail',
              'script_name': 'job_call_detail'}

CONN_ID = 'ift_pkap_247_db'


def get_engine():
    # Загружаем настройки
    connection = BaseHook.get_connection(CONN_ID)

    user = connection.login
    password = connection.password
    host = connection.host
    port = connection.port
    dbname = connection.login
    # dbname = CONN_ID                          # <--------
    return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{dbname}')
    # return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')


def get_last_date_run(**kwargs):
    """Загружаем новые данные"""

    ti = kwargs['ti']

    # Генерируем key_process
    key_process = str(uuid.uuid1())

    # Читаем скрипт из файла
    dir_name = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(f'{dir_name}/sql_call_detail/get_last_date_run.sql', "r", "UTF-8") as f:
        query_get_last_date_run = f.read()

    query_get_last_date_run = query_get_last_date_run \
        .replace('{{ params.schema_name }}', parameters.get('schema_name')) \
        .replace('{{ params.table_name }}', parameters.get('table_name')) \
        .replace('{{ params.script_name }}', parameters.get('script_name'))

    df_last_date_run = pd.read_sql(query_get_last_date_run, get_engine())

    # Сохраняем как список
    script_last_date_run = df_last_date_run.last_time.tolist()          #<--------last_time
    script_last_date_run = [n.strftime("%Y-%m-%d") for n in script_last_date_run]

    id_script = str(df_last_date_run.id.unique()[0])

    print(id_script, script_last_date_run)

    # Сохраняем в XCom
    ti.xcom_push(key='script_last_date_run', value=script_last_date_run)
    ti.xcom_push(key='id_script', value=id_script)
    ti.xcom_push(key='key_process', value=key_process)


def print_last_date_run(**kwargs):
    """Тестовая функция"""
    ti = kwargs['ti']
    # Получаем данные
    id_script = ti.xcom_pull(key='id_script', task_ids='get_last_date_run')
    script_last_date_run = ti.xcom_pull(key='script_last_date_run', task_ids='get_last_date_run')

    print(id_script, script_last_date_run)


with DAG(
        dag_id='call_detail',
        description='Calculate call_detail on synthetic data',
        start_date=days_ago(0, 0, 0, 0, 0),
        schedule_interval='@once',
        catchup=False,
) as dag:
    # Проверяем, что пришли новые данные
    check_last_date_run = SqlSensor(
        task_id='check_last_date_run',
        conn_id=CONN_ID,                                        # <---------
        sql='sql_call_detail/get_last_date_run.sql',
        params=parameters,
        # success=_success_criteria,
        # failure=_failure_criteria,
        fail_on_empty=False,
        poke_interval=1 * 30,  # runs script every 30 sec
        mode='reschedule',
        timeout=60 * 5  # set to 5 minutes, so if the data hasn't arrived by that time, the task fails
    )

    # Выгружаем новые данные
    get_last_date_run = PythonOperator(
        task_id='get_last_date_run',
        python_callable=get_last_date_run,
        provide_context=True,
    )

    # ТЕСТОВЫЙ ОПЕРАТОР - проверка работы x-com
    print_last_date_run = PythonOperator(
        task_id='print_last_date_run',
        python_callable=print_last_date_run,
    )

    # Записываем в таблицу script_log время и статус начала обработки
    update_script_log_start = PostgresOperator(
        task_id="update_script_log_start",
        postgres_conn_id=CONN_ID,                                           # <------
        # sql="""
        # insert into {{ params.schema_name }}.script_log (id_script, date_start, id_status_process, key_process)
        # values ('{{ ti.xcom_pull(task_ids='get_last_date_run', key='id_script') }}',
        #         (current_timestamp at time zone 'UTC-07'),
        #         1,
        #         '{{ ti.xcom_pull(task_ids='get_last_date_run', key='key_process') }}')
        # """,
        sql="""
            insert into {{ params.schema_name }}.script_log (id_script, date_start, id_status_process, key_process)
            values (111111, 
                    (current_timestamp at time zone 'UTC-07'), 
                    1,
                    'kmlfjafdjjiejllkknnm')
            """,
        params=parameters,
    )

    # Создаем витрину для хранения рассчетных данных
    create_view = PostgresOperator(
        task_id="create_view",
        postgres_conn_id=CONN_ID,                                           # <------
        sql="sql_call_detail/create_fv_call_detail.sql",
        params=parameters,
    )

    # check_last_date_run >> \
    get_last_date_run >> print_last_date_run >> update_script_log_start >> create_view

    # check_last_date_run >> get_last_date_run >> update_script_log_start >> get_call_detail >> create_view >> calculate_params >> update_script_log_stop >> delete_temp_table
    #                                                   create_temp_table >> get_call_detail
