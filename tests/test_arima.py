import sys
sys.path.append("src/v1")

import arima as model
import unittest
from unittest.mock import MagicMock

class TestARIMA(unittest.TestCase):

    def setUp(self):
        self.mock = MagicMock()

        self.model = model.ARIMA(
            model_temp=self.mock,
            model_hum=self.mock,
        )

    def test_predict_24hours(self):
        self.mock.predict.return_value = [[x for x in range(24)],]

        fc = self.model.predict(24)

        self.assertEqual(len(fc), 24)

    def test_predict_48hours(self):
        self.mock.predict.return_value = [[x for x in range(48)],]

        fc = self.model.predict(48)

        self.assertEqual(len(fc), 48)

    def test_predict_72hours(self):
        self.mock.predict.return_value = [[x for x in range(72)],]

        fc = self.model.predict(72)

        self.assertEqual(len(fc), 72)
