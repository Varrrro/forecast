from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

import functions as f

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(2),
}

# DAG initialization
dag = DAG(
    'predictions_service',
    default_args=default_args,
    description="Puesta en marcha de un servicio de predicción de humedad y temperatura",
    schedule_interval=None,
)

#### TASKS ####

# Prepare work directory
PrepareWorkdir = BashOperator(
    task_id='prepare_workdir',
    bash_command='mkdir -p /tmp/forecast/arima/',
    dag=dag,
)

# Download temperature CSV
DownloadTemp = BashOperator(
    task_id='download_temp',
    bash_command='curl -o /tmp/forecast/temperature.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
    dag=dag,
)

# Download humidity CSV
DownloadHum = BashOperator(
    task_id='download_hum',
    bash_command='curl -o /tmp/forecast/humidity.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
    dag=dag,
)

# Unzip temperature CSV
UnzipTemp = BashOperator(
    task_id='unzip_temp',
    bash_command='unzip -od /tmp/forecast/ /tmp/forecast/temperature.csv.zip',
    dag=dag,
)

# Unzip humidity CSV
UnzipHum = BashOperator(
    task_id='unzip_hum',
    bash_command='unzip -od /tmp/forecast/ /tmp/forecast/humidity.csv.zip',
    dag=dag,
)

# Merge temperature and humidity datasets
MergeDatasets = PythonOperator(
    task_id='merge_datasets',
    provide_context=True,
    python_callable=f.merge_datasets,
    op_kwargs={
        'temp': '/tmp/forecast/temperature.csv',
        'hum': '/tmp/forecast/humidity.csv',
        'final': '/tmp/forecast/data.csv',
    },
    dag=dag,
)

# Create network for docker containers
CreateDockerNetwork = BashOperator(
    task_id='create_docker_network',
    bash_command='docker network create forecast',
    dag=dag,
)

# Run database docker container
RunDatabaseContainer = BashOperator(
    task_id='run_db_container',
    bash_command='docker run --name="forecast_db" --network="forecast" -e POSTGRES_USER=forecast -e POSTGRES_PASSWORD=forecast -p 5432:5432 -d postgres',
    dag=dag,
)


# Insert data from CSV into database
InsertData = PythonOperator(
    task_id='insert_data',
    provide_context=True,
    python_callable=f.insert_data,
    op_kwargs={
        'file': '/tmp/forecast/data.csv',
    },
    dag=dag,
)

# Train ARIMA temperature model
TrainArimaTemp = PythonOperator(
    task_id='train_arima_temp',
    provide_context=True,
    python_callable=f.train_arima_temp,
    op_kwargs={
        'file': '/tmp/forecast/arima/temp.pkl',
    },
    dag=dag,
)

# Train ARIMA humidity model
TrainArimaHum = PythonOperator(
    task_id='train_arima_hum',
    provide_context=True,
    python_callable=f.train_arima_hum,
    op_kwargs={
        'file': '/tmp/forecast/arima/hum.pkl',
    },
    dag=dag,
)

# Clone git repository
CloneRepository = BashOperator(
    task_id='clone_repository',
    bash_command='if [ -d "/tmp/forecast/code" ]; then rm -Rf /tmp/forecast/code; fi && git clone https://github.com/Varrrro/forecast.git /tmp/forecast/code',
    dag=dag,
)

# Task dependencies
PrepareWorkdir >> DownloadTemp >> UnzipTemp >> MergeDatasets
PrepareWorkdir >> DownloadHum >> UnzipHum >> MergeDatasets

MergeDatasets >> InsertData
CreateDockerNetwork >> RunDatabaseContainer >> InsertData

InsertData >> [TrainArimaTemp, TrainArimaHum]

CloneRepository
