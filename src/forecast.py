import os
import hug

from arima import ARIMA
from autoreg import AutoReg

pickle_path = os.environ['PKL_PATH'] if 'PKL_PATH' in os.environ else '.'

arima = ARIMA(
    path_temp=pickle_path+'/arima/temp.pkl',
    path_hum=pickle_path+'/arima/hum.pkl',
)

autoreg = AutoReg(
    path_temp=pickle_path+'/autoreg/temp.pkl',
    path_hum=pickle_path+'/autoreg/hum.pkl',
)

@hug.get('/prediction/{hours}', versions=1)
def get_prediction_v1(hours: int):
    return arima.predict(hours)


@hug.get('/prediction/{hours}', versions=2)
def get_prediction_v2(hours: int):
    return autoreg.predict(hours)
