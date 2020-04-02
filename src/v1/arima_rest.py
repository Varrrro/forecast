import os
import hug

from arima import ARIMA

pickle_path = os.environ['PKL_PATH'] if 'PKL_PATH' in os.environ else '.'

model = ARIMA(
    path_temp=f'{pickle_path}/temp.pkl',
    path_hum=f'{pickle_path}/hum.pkl',
)

@hug.get('/prediction/{hours}')
def get_prediction(hours: int):
    return model.predict(hours)
