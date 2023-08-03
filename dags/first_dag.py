from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# diccionario con los argumentos por defecto para el DAG
default_args = {
    "owner": "AlvaroCastro",                  # propietario del DAG.
    "retries": "5",                           # número de reintentos en caso de fallo
    "retry_delay": timedelta(minutes=2),      # tiempo de espera entre reintentos (2 minutos)
}

# nueva instancia de la clase DAG con su respectiva configuración y propiedades.
with DAG(
    dag_id='hello_world',                     # ID único para el DAG.
    default_args=default_args,                # Argumentos por defecto para las tareas
    description='Hello World DAG',            # Descripción del DAG.
    start_date=datetime(2023, 8, 3),          # Fecha de inicio del DAG.
    schedule_interval='*/2 * * * *',          # Frecuencia de ejecución (cada 2 minutos).
) as dag:
    # Definicion una tarea utilizando el operador BashOperator
    task_1 = BashOperator(
        task_id="first_dag",                  # ID único para la tarea.
        bash_command='echo Hola mundo 1!'     # Comando Bash a ejecutar
    )