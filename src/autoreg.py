from datetime import datetime

import pickle

class AutoReg:

    def __init__(self, **kwargs):
        if all(k in kwargs for k in ('model_temp', 'model_hum')):
            self.model_temp = kwargs['model_temp']
            self.model_hum = kwargs['model_hum']
        else:
            with open(kwargs['path_temp'], 'rb') as file:
                self.model_temp = pickle.load(file)

            with open(kwargs['path_hum'], 'rb') as file:
                self.model_hum = pickle.load(file)

    def predict(self, periods):
        fc_temp = self.model_temp.predict(24, 23+periods)
        fc_hum = self.model_hum.predict(24, 23+periods)

        curr_hour = datetime.now().hour
        fc = []

        for x in range(periods):
            hour = curr_hour+x+1
            fc.append({
                'hour': str(hour if hour <= 23 else (hour % 24))+':00',
                'temp': fc_temp[24 + x],
                'hum': fc_hum[24 + x],
            })

        return fc
        