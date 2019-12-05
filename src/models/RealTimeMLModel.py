from __future__ import unicode_literals

import os
import pickle
import random
import csv
import datetime
import json
import pprint
import time
from io import StringIO
from urllib.request import urlopen
import json
import numpy as np

import chardet
import pandas as pd
from bson import ObjectId
from marshmallow import Schema, fields
# import built-in validators
from marshmallow.validate import Length
from fbprophet import Prophet

from . import mongo
from ..shared.JSONEncoder import JSONEncoder


class RealTimeMLModel:

    # class constructor
    def __init__(self):
        print('RealTimeMLModel')

    @classmethod
    def makePickleFile(cls, file_name, flag, path, data=""):
        try:
            row = []
            if data == "":
                date_rng = pd.date_range(start='1/1/2018', end='1/02/2018', freq='S')
                count = 0
                for date in date_rng:
                    temp = random.randrange(0, 60, 6)
                    row.append((date, count, temp))
                    count += 1

            df = pd.DataFrame(row)
            df.columns = ['ds', 'id', 'y']
            clean_df = df.drop(['id'], axis=1)
            clean_df.columns = ['ds', 'y']

            print(clean_df.head())
            dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pickle_files'))
            print(dir_path)

            model = cls.fit_predict_model(clean_df)
            if flag:
                if path == "":
                    save_path = dir_path + "/" + file_name + ".pckl"
                else:
                    save_path = path + "/" + file_name + ".pckl"
                print(save_path)
                with open(save_path, 'wb') as fout:
                    pickle.dump(model, fout)

            return_data = {
                "status": 1,
                "message": "Successfully saved .PICKLE file",
                "data": ""
            }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def anomalyDetection(cls, timestamp, objective_field, file_path):
        try:
            row = [timestamp, float(objective_field)]
            col = ['ds', 'y']
            dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pickle_files'))

            if file_path == "":
                path = dir_path + "/test1.pckl"
            else:
                path = file_path
            df = cls.create_data_frmae(row, col)
            print(df)
            pred = cls.make_forecastign(path, df)
            pred = cls.detect_anomalies(pred)
            print(type(pred))
            print(pred)
            return_data = {
                "status": 1,
                "message": "anomalyDetection",
                "data": {
                    'anomaly': json.dumps(pred['anomaly'].iloc[0], cls=NpEncoder)
                }
            }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def create_data_frmae(cls, row, col):
        lst = [row]
        df = pd.DataFrame(lst, columns=col)
        return df

    @classmethod
    def make_forecastign(cls, file, dataframe):
        with open(file, 'rb') as fin:
            m = pickle.load(fin)
        forecast = m.predict(dataframe)
        forecast['fact'] = dataframe['y'].reset_index(drop=True)
        return forecast

    @classmethod
    def fit_predict_model(cls, dataframe, interval_width=0.99, changepoint_range=0.8):
        m = Prophet(daily_seasonality=False, yearly_seasonality=False, weekly_seasonality=False,
                    seasonality_mode='multiplicative',
                    interval_width=interval_width,
                    changepoint_range=changepoint_range)
        m = m.fit(dataframe)
        return m

    @classmethod
    def detect_anomalies(cls, forecast):
        forecasted = forecast[['ds', 'trend', 'yhat', 'yhat_lower', 'yhat_upper', 'fact']].copy()
        # forecast['fact'] = df['y']

        forecasted['anomaly'] = 0
        forecasted.loc[forecasted['fact'] > forecasted['yhat_upper'], 'anomaly'] = 1
        forecasted.loc[forecasted['fact'] < forecasted['yhat_lower'], 'anomaly'] = -1

        # anomaly importances
        forecasted['importance'] = 0
        forecasted.loc[forecasted['anomaly'] == 1, 'importance'] = \
            (forecasted['fact'] - forecasted['yhat_upper']) / forecast['fact']
        forecasted.loc[forecasted['anomaly'] == -1, 'importance'] = \
            (forecasted['yhat_lower'] - forecasted['fact']) / forecast['fact']

        return forecasted

    @classmethod
    def forecasting(cls, periods, file_path):
        try:
            dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pickle_files'))

            if file_path == "":
                path = dir_path + "/test1.pckl"
            else:
                path = file_path

            horizon = int(periods)

            with open(path, 'rb') as fin:
                m2 = pickle.load(fin)

            future2 = m2.make_future_dataframe(periods=horizon)
            forecast2 = m2.predict(future2)
            data = forecast2[['ds', 'yhat', 'yhat_lower', 'yhat_upper']][-horizon:]
            ret = data.to_json(orient='records', date_format='iso')
            print(ret)
            return_data = {
                "status": 1,
                "message": "forecasting",
                "data": ret
            }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data


class makePickleFileSchema(Schema):
    file_name = fields.Str(required=True)
    flag = fields.Int(required=True)
    path = fields.Str(required=True)
    data = fields.Str(required=True)


class anomalyDetectionSchema(Schema):
    timestamp = fields.Str(required=True)
    objective_field = fields.Str(required=True)
    file_path = fields.Str(required=True)


class forecastingSchema(Schema):
    periods = fields.Int(required=True)
    file_path = fields.Str(required=True)


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)
