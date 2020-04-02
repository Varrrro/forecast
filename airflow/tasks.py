from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

import functions as func

# Set base directory
base_dir='/tmp/forecast'

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
    bash_command=f'mkdir -p {base_dir}/data && mkdir -p {base_dir}/models/arima && mkdir -p {base_dir}/models/autoreg',
    dag=dag,
)

# Download temperature CSV
DownloadTemp = BashOperator(
    task_id='download_temp',
    bash_command=f'curl -o {base_dir}/data/temperature.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
    dag=dag,
)

# Download humidity CSV
DownloadHum = BashOperator(
    task_id='download_hum',
    bash_command=f'curl -o {base_dir}/data/humidity.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
    dag=dag,
)

# Unzip temperature CSV
UnzipTemp = BashOperator(
    task_id='unzip_temp',
    bash_command=f'unzip -od {base_dir}/data/ {base_dir}/data/temperature.csv.zip',
    dag=dag,
)

# Unzip humidity CSV
UnzipHum = BashOperator(
    task_id='unzip_hum',
    bash_command=f'unzip -od {base_dir}/data/ {base_dir}/data/humidity.csv.zip',
    dag=dag,
)

# Merge temperature and humidity datasets
MergeDatasets = PythonOperator(
    task_id='merge_datasets',
    provide_context=True,
    python_callable=func.merge_datasets,
    op_kwargs={
        'temp': f'{base_dir}/data/temperature.csv',
        'hum': f'{base_dir}/data/humidity.csv',
        'final': f'{base_dir}/data/merged.csv',
    },
    dag=dag,
)

# Run database docker container
RunDatabaseContainer = BashOperator(
    task_id='run_db_container',
    bash_command='docker run --name="forecast_db" -e POSTGRES_USER=forecast -e POSTGRES_PASSWORD=forecast -p 5432:5432 -d postgres',
    dag=dag,
)


# Insert data from CSV into database
InsertData = PythonOperator(
    task_id='insert_data',
    provide_context=True,
    python_callable=func.insert_data,
    op_kwargs={
        'file': f'{base_dir}/data/merged.csv',
    },
    dag=dag,
)

# Train ARIMA temperature model
TrainArimaTemp = PythonOperator(
    task_id='train_arima_temp',
    provide_context=True,
    python_callable=func.train_arima_temp,
    op_kwargs={
        'file': f'{base_dir}/models/arima/temp.pkl',
    },
    dag=dag,
)

# Train ARIMA humidity model
TrainArimaHum = PythonOperator(
    task_id='train_arima_hum',
    provide_context=True,
    python_callable=func.train_arima_hum,
    op_kwargs={
        'file': f'{base_dir}/models/arima/hum.pkl',
    },
    dag=dag,
)

# Train Autoregressive temperature model
TrainAutoregTemp = PythonOperator(
    task_id='train_autoreg_temp',
    provide_context=True,
    python_callable=func.train_autoreg_temp,
    op_kwargs={
        'file': f'{base_dir}/models/autoreg/temp.pkl',
    },
    dag=dag,
)

# Train Autoregressive humidity model
TrainAutoregHum = PythonOperator(
    task_id='train_autoreg_hum',
    provide_context=True,
    python_callable=func.train_autoreg_hum,
    op_kwargs={
        'file': f'{base_dir}/models/autoreg/hum.pkl',
    },
    dag=dag,
)

# Clone git repository
CloneRepository = BashOperator(
    task_id='clone_repository',
    bash_command=f'if [ -d "{base_dir}/code" ]; then rm -Rf {base_dir}/code; fi && git clone https://github.com/Varrrro/forecast.git {base_dir}/code',
    dag=dag,
)

# Run service tests
RunTests = BashOperator(
    task_id='run_tests',
    bash_command=f'cd {base_dir}/code && python3 -m unittest discover tests',
    dag=dag,
)

# Build ARIMA service image
BuildArimaServiceImage = BashOperator(
    task_id='build_arima_service_image',
    bash_command=f'docker build -t forecast:arima {base_dir}/code/src/v1',
    dag=dag,
)

# Build Autoregressive service image
BuildAutoregServiceImage = BashOperator(
    task_id='build_autoreg_service_image',
    bash_command=f'docker build -t forecast:autoreg {base_dir}/code/src/v2',
    dag=dag,
)

# Build gateway image
BuildGatewayImage = BashOperator(
    task_id='build_gateway_image',
    bash_command=f'docker build -t forecast:gateway {base_dir}/code/src/gateway',
    dag=dag,
)

# Create docker network for container interoperation
CreateDockerNetwork = BashOperator(
    task_id='create_docker_network',
    bash_command='docker network create forecast',
    dag=dag,
)

# Run ARIMA service container
RunArimaServiceContainer = BashOperator(
    task_id='run_arima_service_container',
    bash_command=f'docker run --name="forecast_arima" --network="forecast" -v {base_dir}/models/arima:/models -d forecast:arima',
    dag=dag,
)

# Run Autoregressive service container
RunAutoregServiceContainer = BashOperator(
    task_id='run_autoreg_service_container',
    bash_command=f'docker run --name="forecast_autoreg" --network="forecast" -v {base_dir}/models/autoreg:/models -d forecast:autoreg',
    dag=dag,
)

# Run gateway container
RunGatewayContainer = BashOperator(
    task_id='run_gateway_container',
    bash_command='docker run --name="forecast_gateway" --network="forecast" -p 8000:8080 -d forecast:gateway',
    dag=dag,
)

# Task dependencies
PrepareWorkdir >> DownloadTemp >> UnzipTemp >> MergeDatasets
PrepareWorkdir >> DownloadHum >> UnzipHum >> MergeDatasets

[MergeDatasets, RunDatabaseContainer] >> InsertData

InsertData >> [TrainArimaTemp, TrainArimaHum] >> RunArimaServiceContainer
InsertData >> [TrainAutoregTemp, TrainAutoregHum] >> RunAutoregServiceContainer

CloneRepository >> RunTests >> [CreateDockerNetwork, BuildArimaServiceImage, BuildAutoregServiceImage, BuildGatewayImage]

[CreateDockerNetwork, BuildArimaServiceImage] >> RunArimaServiceContainer
[CreateDockerNetwork, BuildAutoregServiceImage] >> RunAutoregServiceContainer
[BuildGatewayImage, RunArimaServiceContainer, RunAutoregServiceContainer] >> RunGatewayContainer
