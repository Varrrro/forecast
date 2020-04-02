import pandas as pd
import pmdarima as pm
import sqlalchemy
import pickle
import os.path

from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.ar_model import AutoReg

def merge_datasets(**kwargs):
    temp_df = pd.read_csv(kwargs['temp'])
    hum_df = pd.read_csv(kwargs['hum'])

    temp_df = temp_df.dropna(subset=['San Francisco'])
    hum_df = hum_df.dropna(subset=['San Francisco'])

    temp_df = temp_df[['datetime', 'San Francisco']]
    hum_df = hum_df[['datetime', 'San Francisco']]

    temp_df = temp_df.rename(columns={'datetime': 'DATE', 'San Francisco': 'TEMP'})
    hum_df = hum_df.rename(columns={'datetime': 'DATE', 'San Francisco': 'HUM'})

    merged_df = pd.merge(temp_df, hum_df, on='DATE')
    merged_df.to_csv(kwargs['final'], index=False)

def insert_data(**kwargs):
    conn = sqlalchemy.create_engine('postgresql://forecast:forecast@localhost:5432/forecast')
    pd.read_csv(kwargs['file']).to_sql(name='history', con=conn, if_exists='replace', index=False)
    conn.dispose()

def check_models(**kwargs):
    branches = []
    if not os.path.isfile('/tmp/forecast/arima/temp.pkl'):
        branches.append('train_arima_temp')

    if not os.path.isfile('/tmp/forecast/arima/hum.pkl'):
        branches.append('train_arima_hum')

    if not branches:
        return 'clone_repository'
    
    return branches

def train_arima_temp(**kwargs):
    if os.path.isfile(kwargs['file']):
        return
    
    conn = sqlalchemy.create_engine('postgresql://forecast:forecast@localhost:5432/forecast')
    df = pd.read_sql_table('history', conn)

    model = pm.auto_arima(df[['TEMP']], start_p=1, start_q=1,
        test='adf',
        max_p=3, max_q=3,
        m=1,
        d=None,
        seasonal=False,
        start_P=0,
        D=0,
        trace=True,
        error_action='ignore',
        supress_warnings=True,
        stepwise=True)

    with open(kwargs['file'], 'wb') as file:
        pickle.dump(model, file)

    conn.dispose()

def train_arima_hum(**kwargs):
    if os.path.isfile(kwargs['file']):
        return
    
    conn = sqlalchemy.create_engine('postgresql://forecast:forecast@localhost:5432/forecast')
    df = pd.read_sql_table('history', conn)

    model = pm.auto_arima(df[['HUM']], start_p=1, start_q=1,
        test='adf',
        max_p=3, max_q=3,
        m=1,
        d=None,
        seasonal=False,
        start_P=0,
        D=0,
        trace=True,
        error_action='ignore',
        supress_warnings=True,
        stepwise=True)

    with open(kwargs['file'], 'wb') as file:
        pickle.dump(model, file)

    conn.dispose()

def train_autoreg_temp(**kwargs):
    if os.path.isfile(kwargs['file']):
        return
    
    conn = sqlalchemy.create_engine('postgresql://forecast:forecast@localhost:5432/forecast')
    df = pd.read_sql_table('history', conn)

    model = AutoReg(df[['TEMP']], 5).fit()

    with open(kwargs['file'], 'wb') as file:
        pickle.dump(model, file)

    conn.dispose()

def train_autoreg_hum(**kwargs):
    if os.path.isfile(kwargs['file']):
        return
    
    conn = sqlalchemy.create_engine('postgresql://forecast:forecast@localhost:5432/forecast')
    df = pd.read_sql_table('history', conn)

    model = AutoReg(df[['HUM']], 5).fit()

    with open(kwargs['file'], 'wb') as file:
        pickle.dump(model, file)

    conn.dispose()
