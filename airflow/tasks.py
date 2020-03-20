from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from prepro import merge_datasets

default_args = {
    'owner': 'varrrro',
    'email': ['victorvazrod@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0,
}

# DAG initialization
dag = DAG(
    'predictions_service',
    default_args=default_args,
    description="Puesta en marcha de un servicio de predicción de humedad y temperatura",
    schedule_interval=None,
)

#### TASKS ####

# Download temperature CSV
DownloadTemp = BashOperator(
    task_id='download_temp',
    bash_command='curl -o /tmp/temperature.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
    dag=dag,
)

# Download humidity CSV
DownloadHum = BashOperator(
    task_id='download_hum',
    bash_command='curl -o /tmp/humidity.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
    dag=dag,
)

# Unzip temperature CSV
UnzipTemp = BashOperator(
    task_id='unzip_temp',
    bash_command='unzip -d /tmp/ /tmp/temperature.csv.zip',
    dag=dag,
)

# Unzip humidity CSV
UnzipHum = BashOperator(
    task_id='unzip_hum',
    bash_command='unzip -d /tmp/ /tmp/humidity.csv.zip',
    dag=dag,
)

# Merge temperature and humidity datasets
MergeDatasets = PythonOperator(
    task_id='merge_datasets',
    provide_context=True,
    python_callable=merge_datasets('/tmp/temperature.csv', '/tmp/humidity.csv', '/tmp/data.csv'),
    dag=dag,
)