"""
Created on Wed Jan 19 12:23:21 2022

@author: 19551870
"""
import os
import uuid
import codecs
from datetime import timedelta
from sqlalchemy import create_engine, text
import pandas as pd

from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from airflow.hooks.base import BaseHook
from airflow import AirflowException

from airflow.decorators import dag, task

# from airflow.models import Variable


parameters = {'schema_name': 'pkap_247_sch',
              'log_schema_name': 'pltfrm_247_sch',
              'table_name': 'call_detail',
              'mart_name': 'mart_call_detail',
              'script_name': 'job_call_detail',
              'stat_process': {'start': 1,
                               'fail': 2,
                               'stop': 3}
              }

CONN_ID = 'ift_pkap_247_db'


@dag(description='Calculate call_detail on synthetic data',
     start_date=days_ago(0, 0, 0, 0, 0),
     schedule_interval='@once',
     catchup=False,
     dagrun_timeout=timedelta(minutes=50),)
def process_call_detail():
    
    def _get_engine():
        # Загружаем настройки
        connection = BaseHook.get_connection(CONN_ID)
    
        user = connection.login
        password = connection.password
        host = connection.host
        port = connection.port
        dbname = connection.schema
    
        return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}',
                              pool_pre_ping=True)
    
    def _read_query_text(file_name: str) -> str:
        
        # 
        schema_name = parameters.get('schema_name')
        log_schema_name = parameters.get('log_schema_name')
        table_name = parameters.get('table_name')
        mart_name = parameters.get('mart_name')
        script_name = parameters.get('script_name')
        
        # Читаем скрипт из файла
        dir_name = os.path.abspath(os.path.dirname(__file__))
        with codecs.open(f'{dir_name}/{file_name}', "r", "UTF-8") as f:
            query_text = f.read()
        
        query_text = query_text \
            .replace('{{params.schema_name}}', schema_name) \
            .replace('{{params.log_schema_name}}', log_schema_name) \
            .replace('{{params.table_name}}', table_name) \
            .replace('{{params.mart_name}}', mart_name) \
            .replace('{{params.script_name}}', script_name)
        
        return query_text
    
    
    def _update_log_error(log_table_name, where_table_name, process_key, exception_text, description_text):
        """
        Записываем в таблицу логирования статус ошибки.
        script_log
        table_log
        Записываем в таблицу error_log описание ошибки.
        """
           
        schema_name = parameters.get('log_schema_name')
        # table_name = parameters.get('table_name')
        # script_name = parameters.get('script_name')
        
        if log_table_name == 'script_log':
            where_str = f"process_key = '{process_key}'"
        else:
            where_str = f"id_table = (select id from {schema_name}.tables_info where table_name = '{where_table_name}') and process_key = '{process_key}'"
        
        query_error = f"""
            UPDATE {schema_name}.{log_table_name}
            SET 
                date_end = (current_timestamp at time zone 'UTC-07'),
                id_status_process = {parameters.get('stat_process').get('fail')}
            WHERE 
                {where_str}
            """
        
        engine = _get_engine()
        with engine.connect() as connection:
            try:
                connection.execute(text(query_error))
            except Exception:
                raise AirflowException(f"Error inserting status into {log_table_name}")  
        
        # Записываем в таблицу error_log описание ошибки.
        exception_text = exception_text[:10000].replace("'", "-")
        description_text = description_text[:255].replace("'", "-")
        
        query_log_err = f"""    
            INSERT INTO {schema_name}.error_log (type_log, id_log, exception_text, description) 
            VALUES ('{log_table_name[:-4]}',
                    (select id FROM {schema_name}.{log_table_name} where {where_str}),
                    '{exception_text}',
                    '{description_text}'
                   )
            """
            
        with engine.connect() as connection:
            try:
                connection.execute(text(query_log_err))
            except Exception:
                raise AirflowException(f"Error inserting status from {log_table_name} into error_log")  
            
            
    def _update_log_success(log_table_name, where_table_name, process_key):
        """
        Записываем в таблицу логирования статус успешного окончания.
        script_log
        table_log
        """
           
        schema_name = parameters.get('log_schema_name')
        
        if log_table_name == 'script_log':
            where_str = f"process_key = '{process_key}'"
        else:
            where_str = f"id_table = (select id from {schema_name}.tables_info where table_name = '{where_table_name}') and process_key = '{process_key}'"
        
        query_success = f"""
            UPDATE {schema_name}.{log_table_name}
            SET 
                date_end = (current_timestamp at time zone 'UTC-07'),
                id_status_process = {parameters.get('stat_process').get('stop')}
            WHERE 
                {where_str}
            """
        
        engine = _get_engine()
        with engine.connect() as connection:
            connection.execute(text(query_success))

        
    def _start_table_log(table_name, process_key):
        """Создаем запись в table_log о начале записи данных в таблицу table_name"""
        
        schema_name = parameters.get('log_schema_name')      
        
        query_start_log = f"""    
            INSERT INTO {schema_name}.table_log (id_table, date_start, id_status_process, process_key) 
            VALUES ((select id from {schema_name}.tables_info where table_name = '{table_name}'), 
                    (current_timestamp at time zone 'UTC-07'),
                    {parameters.get('stat_process').get('start')}, 
                    '{process_key}')
            """
        
        engine = _get_engine()
        with engine.connect() as connection:
            try:
                connection.execute(text(query_start_log))
            except Exception as ex:
                descr = "Error at create start status into table_log"
                _update_log_error('script_log', '', process_key, str(ex), descr)
                # raise AirflowException("Error at create start status into table_log")   
    

    check_last_date_run = SqlSensor(
        task_id='check_last_date_run',
        conn_id=CONN_ID,
        sql='sql_call_detail/get_last_date_run.sql',
        params=parameters,
        fail_on_empty=False,
        poke_interval=1 * 30,  # runs script every 30 sec
        mode='reschedule',
        timeout=60 * 5,  # set to 5 minutes, so if the data hasn't arrived by that time, the task fails
        soft_fail=True
    )

    @task()
    def check_scripts_info():
        """
        Проверяем, что в таблице scripts_info есть запись с нашим скриптом,
        если ее нет, то создаем
        """

        script_name = parameters.get('script_name')
        log_schema_name = parameters.get('log_schema_name')

        # Проверяем есть ли данные
        query_check_si = f"""
                select id
                FROM   {log_schema_name}.scripts_info
                where  script_name = '{script_name}'
                """

        engine = _get_engine()
        # Посылаем запрос
        try:
            df_scr_inf = pd.read_sql(query_check_si, engine)
        except Exception as ex:
            descr = "Error at check_scripts_info"
            raise AirflowException(descr)

        if df_scr_inf.shape[0] == 0:
            query_insert = f"""
            INSERT INTO {log_schema_name}.scripts_info (script_name, customer, last_date_run) 
            VALUES ('{script_name}','ЦКР', '1970-01-01 00:00:00'::timestamp)
            """

            with engine.connect() as connection:
                try:
                    connection.execute(text(query_insert))
                except Exception as ex:
                    descr = "Error at inserting first row in scripts_info"
                    raise AirflowException(descr)

    @task()
    def check_scripts_and_tables():
        """
        Проверяем, что в таблице scripts_and_tables есть запись,
        если ее нет, то создаем
        """
        script_name = parameters.get('script_name')
        table_name = parameters.get('table_name')
        log_schema_name = parameters.get('log_schema_name')

        # Проверяем есть ли данные
        query_check_st = f"""
                    SELECT id
                    FROM   {log_schema_name}.scripts_and_tables
                    where  id_table = (select id from {log_schema_name}.tables_info where table_name = '{table_name}');
                    """

        engine = _get_engine()
        # Посылаем запрос
        try:
            df_snt_inf = pd.read_sql(query_check_st, engine)
        except Exception as ex:
            descr = "Error at check_scripts_and_tables"
            raise AirflowException(descr)

        # Еслии данных нет, то записываем
        if df_snt_inf.shape[0] == 0:
            query_insert = f"""
                INSERT INTO {log_schema_name}.scripts_and_tables (id_script, id_table) 
                VALUES ((select id from {log_schema_name}.scripts_info where script_name = '{script_name}'),
                        (select id from {log_schema_name}.tables_info where table_name = '{table_name}'));
                """

            with engine.connect() as connection:
                try:
                    connection.execute(text(query_insert))
                except Exception as ex:
                    descr = "Error at inserting first row in scripts_and_tables"
                    raise AirflowException(descr)


    @task(multiple_outputs=True)
    def get_last_date_run():
        """Загружаем новые данные"""
        # Генерируем process_key
        process_key = str(uuid.uuid1())
        # Читаем скрипт из файла
        query_get_last_date_run = _read_query_text('sql_call_detail/get_last_date_run.sql')
                  
        engine = _get_engine()
        with engine.connect() as connection:
            df_last_date_run = pd.read_sql(query_get_last_date_run, connection)
        # Закрываем соединение
        engine.dispose()

        # Сохраняем как список
        day_count = int(df_last_date_run['day_count'].iloc[0])
        id_script = str(df_last_date_run['id'].iloc[0])
        
        return {'day_count': day_count,
                'id_script': id_script,
                'process_key': process_key}


    @task()
    def create_temp_table(last_date_dict: dict):
        """Создаем временную таблицу для хранения новых результатов"""
        
        schema_name = parameters.get('schema_name')
        table_name = parameters.get('table_name')
        
        process_key = last_date_dict.get('process_key')
                
        
        query_create = f"""create table if not exists 
            {schema_name}.temp_{table_name} 
            (LIKE {schema_name}.{table_name})"""
            
        engine = _get_engine()
        with engine.connect() as connection:
            try:
                connection.execute(text(query_create))
            except Exception as ex:
                descr = "Error at create_temp_table"
                _update_log_error('script_log', '', process_key, str(ex), descr)
                raise AirflowException(descr)
            finally:
                # Закрываем соединение
                engine.dispose()
                
    @task()
    def start_temp_table_log(last_date_dict: dict):
        """Создаем запись в table_log о начале записи данных в temp_call_detail"""
                
        table_name = parameters.get('table_name')
        process_key = last_date_dict.get('process_key')
        # Записываем данные в таблицу
        _start_table_log(f'temp_{table_name}', process_key)

    @task(multiple_outputs=True)
    def get_call_detail(last_date_dict: dict):
        """Получаем новые данные из call_detail и записываем их во временную таблицу"""
        
        # Получаем данные из XCom
        process_key = last_date_dict.get('process_key')
        day_count = last_date_dict.get('day_count')
        
        #
        schema_name = parameters.get('schema_name')
        table_name = parameters.get('table_name')
        
        # Обрабатываем call_detail
        # для получения последних данных
        query_call_detail = f"""SELECT *
            FROM 
                {schema_name}.{table_name}
            WHERE 
                date_trunc('day', start_time) >= date_trunc('day', current_timestamp at time zone 'UTC-07') - INTERVAL '{day_count} DAY'
            """
        
        engine = _get_engine()
        # Посылаем запрос
        try:
            df_call_detail = pd.read_sql(query_call_detail, engine)
        except Exception as ex:
            descr = "Error at get_call_detail in read data"
            _update_log_error('script_log',  '', process_key, str(ex), descr)
            _update_log_error('table_log', f'{table_name}', process_key, str(ex), descr)
            raise AirflowException(descr)    
       
        # Сохраняем данные во временную таблицу с ЗАМЕНОЙ всех результатов
        try:
            df_call_detail.to_sql(f'temp_{table_name}', 
                                  engine, 
                                  schema=schema_name, 
                                  if_exists='replace', 
                                  index=False)
        except Exception as ex:
            descr = "Error at get_call_detail in insert data to temp table"
            _update_log_error('script_log', '', process_key, str(ex), descr)
            _update_log_error('table_log', f'temp_{table_name}', process_key, str(ex), descr)
            raise AirflowException(descr)
        else:
            _update_log_success('table_log', f'temp_{table_name}', process_key)
                
        return last_date_dict
    
    
    @task()
    def start_mart_table_log(last_date_dict: dict):
        """Создаем запись в table_log о начале записи данных в mart_call_detail"""
        
        table_name = parameters.get('table_name')
        process_key = last_date_dict.get('process_key')
        # Записываем данные в таблицу
        _start_table_log(f'mart_{table_name}', process_key)
    
    
    @task()
    def clear_mart_last_date(last_date_dict: dict):
        """Удаляем из витрины данные за те дни, за которые есть поставка сейчас"""
        
        mart_name = parameters.get('mart_name')
        # Получаем данные из XCom
        process_key = last_date_dict.get('process_key')
        day_count = last_date_dict.get('day_count')
        
        # Читаем скрипт из файла
        query_clear = _read_query_text('sql_call_detail/clear_mart.sql')
        query_clear = query_clear.replace('{{day_count}}', str(day_count))
                
        engine = _get_engine()       
        with engine.connect() as connection:
            try:
                connection.execute(text(query_clear))
            except Exception as ex:
                descr = "Error at clear_mart"
                _update_log_error('script_log', '', process_key, str(ex), descr)
                _update_log_error('table_log', f'{mart_name}', process_key, str(ex), descr)
                raise AirflowException(descr)
        
    @task()
    def calculate_mart_call_detail(last_date_dict: dict):
        """Расчет параметров для витрины"""
        
        # Получаем данные из XCom
        process_key = last_date_dict.get('process_key')
        
        schema_name = parameters.get('schema_name')
        mart_name = parameters.get('mart_name')
        
        # Читаем скрипт из файла
        query_call_detail = _read_query_text('sql_call_detail/call_detail_result.sql')
        
        engine = _get_engine()
        # Запрос
        try:
            df_call_detail = pd.read_sql(query_call_detail, engine)
        except Exception as ex:
            descr = "Error at calculate_mart_call_detail in read data"
            _update_log_error('script_log', '', process_key, str(ex), descr)
            raise AirflowException(descr)
        
        # Сохраняем данные в витрину с 
        # ЗАМЕНОЙ всех результатов - replace
        # ДОБАВЛЕНИЕМ - append 
        try:
            df_call_detail.to_sql(f'{mart_name}', 
                                  engine, 
                                  schema=schema_name,
                                  if_exists='append', 
                                  index=False)
        except Exception as ex:
            descr = "Error at calculate_mart_call_detail in write to mart"
            _update_log_error('script_log', '', process_key, str(ex), descr)
            _update_log_error('table_log', f'{mart_name}', process_key, str(ex), descr)
            raise AirflowException(descr)
        else:
            _update_log_success('table_log', f'{mart_name}', process_key)
    

    # Записываем в таблицу script_log время и статус начала обработки
    start_script_log = PostgresOperator(
        task_id="start_script_log",
        postgres_conn_id=CONN_ID,
        sql="""
        insert into {{params.log_schema_name}}.script_log (id_script, date_start, id_status_process, process_key)
        values ('{{ti.xcom_pull(task_ids='get_last_date_run', key='id_script')}}',
                (current_timestamp at time zone 'UTC-07'),
                {{params.stat_process.start}},
                '{{ti.xcom_pull(task_ids='get_last_date_run', key='process_key')}}')
        """,
        params=parameters,
    )

    # Создаем витрину для хранения рассчетных данных
    create_mart = PostgresOperator(
        task_id="create_mart",
        postgres_conn_id=CONN_ID,
        sql="sql_call_detail/create_mart_call_detail.sql",
        params=parameters,
    )
    
    # Записываем в таблицу script_log время и статус окончания обработки
    stop_script_log = PostgresOperator(
        task_id="stop_script_log",
        postgres_conn_id=CONN_ID,
        sql="""
        UPDATE {{params.log_schema_name}}.script_log
        SET date_end = (current_timestamp at time zone 'UTC-07'), id_status_process = {{params.stat_process.stop}}
        WHERE process_key = '{{ti.xcom_pull(task_ids='get_last_date_run', key='process_key')}}'
        """,
        params=parameters,
    )

    # Dependencies between TaskFlow functions
    chk_scr_info = check_scripts_info()
    chk_scr_n_tbl = check_scripts_and_tables()
    last_date_run = get_last_date_run()
    create_temp_tbl = create_temp_table(last_date_run)
    log_temp = start_temp_table_log(last_date_run)
    get_call_dtl = get_call_detail(last_date_run)
    log_mart = start_mart_table_log(get_call_dtl)
    clr_mart = clear_mart_last_date(get_call_dtl)
    calculated_mart = calculate_mart_call_detail(get_call_dtl)

    # # Dependencies between traditional tasks and TaskFlow functions.
    chk_scr_info >> chk_scr_n_tbl >> check_last_date_run >> last_date_run >> start_script_log >> create_temp_tbl >> log_temp >> get_call_dtl
    get_call_dtl >> create_mart >> log_mart >> clr_mart >> calculated_mart 
    calculated_mart >> stop_script_log

 # get_last_date_run >> update_script_log_start >> get_call_detail >> create_mart >> clear_mart_last_date >> calculate_mart_call_detail >> update_script_log_stop >> delete_temp_table
 #                            create_temp_table >> get_call_detail


call_detail_task_flow = process_call_detail()
