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

from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from airflow.hooks.base_hook import BaseHook

from airflow.decorators import dag, task


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
    dbname = connection.schema
    # dbname = CONN_ID                          # <--------

    return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{dbname}')
    # return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')


@dag(description='Calculate call_detail on synthetic data',
     start_date=days_ago(0, 0, 0, 0, 0),
     schedule_interval='@once',
     catchup=False,)
def process_call_detail():

    check_last_date_run = SqlSensor(
        task_id='check_last_date_run',
        conn_id=CONN_ID,                                        # <---------
        sql='sql_call_detail/get_last_date_run.sql',
        params=parameters,
        fail_on_empty=False,
        poke_interval=1 * 30,  # runs script every 30 sec
        mode='reschedule',
        timeout=60 * 5  # set to 5 minutes, so if the data hasn't arrived by that time, the task fails
    )

    # Выгружаем новые данные
    @task(multiple_outputs=True)
    def get_last_date_run():
        """Загружаем новые данные"""
        # Генерируем key_process
        key_process = str(uuid.uuid1())
        # Читаем скрипт из файла
        dir_name = os.path.abspath(os.path.dirname(__file__))
        with codecs.open(f'{dir_name}/sql_call_detail/get_last_date_run.sql', "r", "UTF-8") as f:
            query_get_last_date_run = f.read()

        query_get_last_date_run = query_get_last_date_run \
            .replace('{{params.schema_name}}', parameters.get('schema_name')) \
            .replace('{{params.table_name}}', parameters.get('table_name')) \
            .replace('{{params.script_name}}', parameters.get('script_name'))

        df_last_date_run = pd.read_sql(query_get_last_date_run, get_engine())

        # Сохраняем как список
        script_last_date_run = df_last_date_run.last_time.tolist()                      # <--------
        script_last_date_run = [n.strftime("%Y-%m-%d") for n in script_last_date_run]

        id_script = str(df_last_date_run.id.unique()[0])

        return {'script_last_date_run': script_last_date_run,
                'id_script': id_script,
                'key_process': key_process}

    # ТЕСТОВЫЙ ОПЕРАТОР - проверка работы x-com
    @task()
    def print_last_date_run(last_dict: dict):
        """Тестовая функция"""
        # Получаем данные
        id_script = last_dict.get('id_script')
        script_last_date_run = last_dict.get('script_last_date_run')

        print(id_script, script_last_date_run)

    # Записываем в таблицу script_log время и статус начала обработки
    update_script_log_start = PostgresOperator(
        task_id="update_script_log_start",
        postgres_conn_id=CONN_ID,                                          # <------
        sql="""
        insert into {{ params.schema_name }}.script_log (id_script, date_start, id_status_process, key_process)
        values ('{{ti.xcom_pull(task_ids='get_last_date_run', key='id_script')}}',
                (current_timestamp at time zone 'UTC-07'),
                1,
                '{{ti.xcom_pull(task_ids='get_last_date_run', key='key_process')}}')
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

    # Dependencies between TaskFlow functions
    last_date_run = get_last_date_run()
    print_ldr = print_last_date_run(last_date_run)

    # Dependencies between traditional tasks and TaskFlow functions.
    check_last_date_run >> last_date_run >> print_ldr >> update_script_log_start >> create_view


call_detail_task_flow = process_call_detail()
