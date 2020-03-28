from statsmodels.tsa.arima_model import ARIMA
from datetime import datetime

import pickle

class Model:
    def __init__(self, path_temp, path_hum):
        with open(path_temp, 'rb') as file:
            self.model_temp = pickle.load(file)

        with open(path_hum, 'rb') as file:
            self.model_hum = pickle.load(file)

    def predict(self, period):
        fc_temp = self.model_temp.predict(n_periods=period, return_conf_int=True)
        fc_hum = self.model_hum.predict(n_periods=period, return_conf_int=True)

        curr_hour = datetime.now().hour
        fc = []

        for x in range(period):
            hour = curr_hour+x+1
            fc.append({
                'hour': str(hour if hour <= 23 else (hour % 24))+':00',
                'temp': fc_temp[0][x],
                'hum': fc_hum[0][x],
            })

        return fc
    