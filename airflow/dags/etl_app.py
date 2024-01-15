from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.models import Connection
from airflow import settings

import logging

logging.basicConfig(
    format='%(levelname)s: %(asctime)s - %(message)s', 
    datefmt='%d-%b-%y %H:%M:%S', 
    level=logging.INFO
    )

def create_conn(conn_id, conn_type, host, login=None, password=None, port=None, schema=None, extra=None):
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        port=port,
        schema=schema,
        password=password,
        login=login,
        extra=extra
    )
    session = settings.Session()
    conn_name = session\
    .query(Connection)\
    .filter(Connection.conn_id == conn.conn_id)\
    .first()

    if str(conn_name) == str(conn_id):
        return logging.info(f"Connection {conn_id} already exists")

    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {conn_id} is created')


default_args = {
    'owner': 'João Peixoto',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    default_args=default_args,
    dag_id='etl_app',
    start_date=datetime(2023, 12, 12),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    create_postgres_conn = PythonOperator(
        task_id="create_postgres_conn", 
        python_callable=create_conn,
        op_kwargs={
            "conn_id": "imdb_postgres", 
            "conn_type": "postgres",
            "host": "imdb_postgres",
            "login": 'admin',
            "password": 'password',
            "schema": "imdb",
            "port": 5432,
        },
    )

    create_spark_conn = PythonOperator(
        task_id="create_spark_conn", 
        python_callable=create_conn,
        op_kwargs={
            "conn_id": "spark_local", 
            "conn_type": "spark",
            "host": "local[*]"
        },
    )   

    create_table_actors_actress_kpis = PostgresOperator(
    task_id="create_table_actors_actress_kpis",
    postgres_conn_id="imdb_postgres",
    sql="""
    CREATE TABLE IF NOT EXISTS actors_actress_kpis (
        nconst TEXT PRIMARY KEY,
        role TEXT,
        name TEXT,
        average_rating NUMERIC(3, 2),
        total_run_time_minutes INTEGER,
        total_titles_as_principal INTEGER

    );

    CREATE INDEX IF NOT EXISTS idx_actors_actress_kpis
    ON actors_actress_kpis(role);
    """,
    )

    create_table_etl_pipeline_status = PostgresOperator(
    task_id="create_table_etl_pipeline_status",
    postgres_conn_id="imdb_postgres",
    sql="""
    CREATE TABLE IF NOT EXISTS etl_pipeline_status (
    stage VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    date DATE NOT NULL,
    status VARCHAR NOT NULL,
    PRIMARY KEY (stage, table_name, date, status)
    );
    """,
    )

    
    caching = SparkSubmitOperator(
        task_id='caching',
        application='/opt/airflow/etl_apps/caching.py',
        conn_id = 'spark_local',
        driver_memory = '4g',
        executor_memory  = '4g',
        verbose=1,
        conf={
            "spark.submit.deployMode": "client",
        # Additional Spark configuration properties
            },
        application_args=[],
        env_vars={
            'SPARK_HOME': '/opt/spark',  # Set SPARK_HOME environment variable
            # Additional environment variables
        },
        packages="io.delta:delta-core_2.12:2.1.0",
        dag=dag
    )

    landing_zone_ingestion = SparkSubmitOperator(
        task_id='landing_zone_ingestion',
        application='/opt/airflow/etl_apps/landing_zone_ingestion.py',
        conn_id='spark_local',
        driver_memory = '4g',
        executor_memory  = '4g',
        verbose=1,
        conf={
            "spark.submit.deployMode": "client",
        },
        packages="io.delta:delta-core_2.12:2.1.0",
        dag=dag
    )

    cleansed_dim_names = SparkSubmitOperator(
        task_id='cleansed_dim_names',
        application='/opt/airflow/etl_apps/cleansed_dim_names.py',
        conn_id='spark_local',
        driver_memory = '4g',
        executor_memory  = '4g',
        verbose=1,
        conf={
            "spark.submit.deployMode": "client",
        },
        packages="io.delta:delta-core_2.12:2.1.0",
        dag=dag
    )

    cleansed_dim_titles = SparkSubmitOperator(
        task_id='cleansed_dim_titles',
        application='/opt/airflow/etl_apps/cleansed_dim_titles.py',
        conn_id='spark_local',
        driver_memory = '4g',
        executor_memory  = '4g',
        verbose=1,
        conf={
            "spark.submit.deployMode": "client",
        },
        packages="io.delta:delta-core_2.12:2.1.0",
        dag=dag
    )

    cleansed_fact_title_principals = SparkSubmitOperator(
        task_id='cleansed_fact_title_principals',
        application='/opt/airflow/etl_apps/cleansed_fact_title_principals.py',
        conn_id='spark_local',
        driver_memory = '4g',
        executor_memory  = '4g',
        verbose=1,
        conf={
            "spark.submit.deployMode": "client",
        },
        packages="io.delta:delta-core_2.12:2.1.0",
        dag=dag
    )

    aggregated_obt_api_use_case = SparkSubmitOperator(
        task_id='aggregated_obt_api_use_case',
        application='/opt/airflow/etl_apps/aggregated_obt_api_use_case.py',
        conn_id='spark_local',
        driver_memory = '4g',
        executor_memory  = '4g',
        verbose=1,
        conf={
            "spark.submit.deployMode": "client",
        },
        packages="io.delta:delta-core_2.12:2.1.0,org.postgresql:postgresql:42.7.1",
        dag=dag
    )

    write_metadata_table = SparkSubmitOperator(
        task_id='write_metadata_table',
        application='/opt/airflow/etl_apps/write_table_etl_pipeline_status.py',  # Path to your Spark script
        conn_id='spark_local',
            driver_memory = '4g',
            executor_memory  = '4g',
            verbose=1,
            conf={
                "spark.submit.deployMode": "client",
            },
            packages="io.delta:delta-core_2.12:2.1.0,org.postgresql:postgresql:42.7.1",
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE  # Run regardless of upstream task status
    )
 
    create_postgres_conn >> [create_table_actors_actress_kpis, create_table_etl_pipeline_status]
    [create_table_actors_actress_kpis, create_table_etl_pipeline_status] >> create_spark_conn
    create_spark_conn >> caching >> landing_zone_ingestion >> [cleansed_dim_names, cleansed_dim_titles, cleansed_fact_title_principals]
    [cleansed_dim_names, cleansed_dim_titles, cleansed_fact_title_principals] >> aggregated_obt_api_use_case 
    aggregated_obt_api_use_case >> write_metadata_table
    
    #add langing zone after