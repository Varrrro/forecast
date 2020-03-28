import hug
import sqlalchemy

from model import Model

model = Model(path_temp='temp.pkl', path_hum='hum.pkl')

@hug.get('/prediction/24hours')
def prediction24():
    return model.predict(24)

@hug.get('/prediction/48hours')
def prediction48():
    return model.predict(48)

@hug.get('/prediction/72hours')
def prediction72():
    return model.predict(72)
