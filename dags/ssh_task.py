from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    'owner': 'AlvaroCastro',
    'depends_on_past': True,
    'email': ['alvaro.castro@qbit.com.mx'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG(
    dag_id='testing_stuff',
    default_args=default_args,
    schedule_interval='*/3 * * * *',
    dagrun_timeout=timedelta(seconds=120)
    )

# Step 1 - Dump data from postgres databases
t1_bash = """
        mkdir /home/acastro/test1/test2 
    """

t2_bash = """
    cd /home/acastro
    echo 'Hello World' > ejemplo.txt
"""
t1 = SSHOperator(
    ssh_conn_id='servidor_test',
    task_id='test_ssh_operator_1',
    command=t1_bash,
    dag=dag)

t2 =  SSHOperator(
    ssh_conn_id='servidor_test',
    task_id='test_ssh_operator_2',
    command=t2_bash,
    dag=dag)

t1 >> t2