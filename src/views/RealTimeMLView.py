import logging

from flask import request, Blueprint, jsonify
from flask_restplus import Resource

from ..models.RealTimeMLModel import makePickleFileSchema, RealTimeMLModel, anomalyDetectionSchema, forecastingSchema
from ..serializers.RealTimeMLSerialize import initiateProducer, makePickle, anomalyDetection, forecasting
from ..shared.APIResponseConfig import APIResponseConfig
from ..shared.restplus import api

log = logging.getLogger(__name__)
raw_data_api = Blueprint('raw_data_api', __name__)
ns = api.namespace('Bions/RealTimeML', description='Activities for Real Time ML')


@ns.route('/makePickleFile')
class makePickleFile(Resource):

    @api.expect(makePickle, validate=False)
    def post(self):
        # validate the received values
        if request.method == 'POST':
            create_project_schema = makePickleFileSchema()
            errors = create_project_schema.validate(request.json)
            if errors:
                resp = APIResponseConfig.TakeJsonResponse(2, errors)
            else:
                _json = request.json
                _flag = _json['flag']
                _path = _json['path']
                _data = _json['data']
                _file_name = _json['file_name']
                status = RealTimeMLModel.makePickleFile(_file_name, _flag, _path, _data)
                if status['status']:
                    resp = APIResponseConfig.TakeJsonResponse(1, status['message'], status['data'])
                else:
                    resp = APIResponseConfig.TakeJsonResponse(4, status['message'])
        else:
            resp = APIResponseConfig.TakeJsonResponse(6, "")
        return resp


@ns.route('/anomalyDetection')
class anomalyDetection(Resource):

    @api.expect(anomalyDetection, validate=False)
    def post(self):
        # validate the received values
        if request.method == 'POST':
            create_project_schema = anomalyDetectionSchema()
            errors = create_project_schema.validate(request.json)
            if errors:
                resp = APIResponseConfig.TakeJsonResponse(2, errors)
            else:
                _json = request.json
                _timestamp = _json['timestamp']
                _objective_field = _json['objective_field']
                _file_path = _json['file_path']
                status = RealTimeMLModel.anomalyDetection(_timestamp, _objective_field, _file_path)
                if status['status']:
                    resp = APIResponseConfig.TakeJsonResponse(1, status['message'], status['data'])
                else:
                    resp = APIResponseConfig.TakeJsonResponse(4, status['message'])
        else:
            resp = APIResponseConfig.TakeJsonResponse(6, "")
        return resp


@ns.route('/Forecasting')
class Forecasting(Resource):

    @api.expect(forecasting, validate=False)
    def post(self):
        # validate the received values
        if request.method == 'POST':
            create_project_schema = forecastingSchema()
            errors = create_project_schema.validate(request.json)
            if errors:
                resp = APIResponseConfig.TakeJsonResponse(2, errors)
            else:
                _json = request.json
                _periods = _json['periods']
                _file_path = _json['file_path']
                status = RealTimeMLModel.forecasting(_periods, _file_path)
                if status['status']:
                    resp = APIResponseConfig.TakeJsonResponse(1, status['message'], status['data'])
                else:
                    resp = APIResponseConfig.TakeJsonResponse(4, status['message'])
        else:
            resp = APIResponseConfig.TakeJsonResponse(6, "")
        return resp
