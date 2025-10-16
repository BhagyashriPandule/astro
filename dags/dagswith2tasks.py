#DAG with task 2
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import time 

def common_func():
    #log.info('Running Task:',task)
    time.sleep(10)
    print('Runnng Task')
# 1. Define the DAG's arguments (settings)
with DAG(
    dag_id="two_task_sequential_dag1",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule='0 0,6,12,18 * * *',
    catchup=False,
    tags=["example", "sequential"],
) as dag:
    
    # 2. Define Task A
    task_a = PythonOperator(
        task_id="task1",
        python_callable=common_func,
    )
    
    # 3. Define Task B
    task_b = PythonOperator(
        task_id="task2",
        python_callable=common_func,
    )
    
    # 4. Define the Task Flow (Dependency)
    # This line sets the dependency: task_a MUST run before task_b.
    [task_a,task_b]