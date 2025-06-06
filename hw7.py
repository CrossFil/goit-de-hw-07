from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql_sensor import SqlSensor
from datetime import datetime, timedelta
import random
import time

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='medal_count_pipeline',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description='Pipeline for counting medals and inserting to MySQL with branching and sensor'
) as dag:

    # 1. Створення таблиці, якщо не існує
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='goit_mysql_db_crossfil',
        sql="""
        CREATE TABLE IF NOT EXISTS medal_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 2. Випадковий вибір медалі
    def choose_medal():
        medal = random.choice(['Bronze', 'Silver', 'Gold'])
        return medal

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=choose_medal
    )

    # 3. Branch — вибір шляху
    def branch_medal(**context):
        chosen = context['ti'].xcom_pull(task_ids='pick_medal')
        return f'calc_{chosen}'

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=branch_medal,
        provide_context=True
    )

    # 4. Підрахунок кількості для кожної медалі
    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id='goit_mysql_db_crossfil',
        sql="""
        INSERT INTO medal_stats (medal_type, count)
        SELECT 'Bronze', COUNT(*) 
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id='goit_mysql_db_crossfil',
        sql="""
        INSERT INTO medal_stats (medal_type, count)
        SELECT 'Silver', COUNT(*) 
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id='goit_mysql_db_crossfil',
        sql="""
        INSERT INTO medal_stats (medal_type, count)
        SELECT 'Gold', COUNT(*) 
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """
    )

    # 5. Затримка
    def delayed():
        time.sleep(25)
        return

    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=delayed
    )


    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='goit_mysql_db_crossfil',
        sql="""
        SELECT 1 FROM medal_stats
        WHERE created_at >= NOW() - INTERVAL 30 SECOND
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        timeout=60,
        poke_interval=10,
        mode='poke'
    )

    # Визначення залежностей
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness
