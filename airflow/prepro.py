import pandas as pd
import sqlalchemy

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
    engine = sqlalchemy.create_engine('postgresql://forecast:forecast@localhost:5432/forecast')
    pd.read_csv(kwargs['file']).to_sql(name='history', con=engine, if_exists='replace', index=False)
    engine.dispose()
