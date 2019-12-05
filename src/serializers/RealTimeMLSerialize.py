from flask_restplus import fields
from ..shared.restplus import api

initiateProducer = api.model('flag', {
    'flag': fields.String(required=True, description='Flag for Kafka Producer'),
})

makePickle = api.model('makePickle', {
    'file_name': fields.String(required=True, description='Flag for Pickle'),
    'flag': fields.Integer(required=True, description='Flag for Pickle'),
    'path': fields.String(required=True, description='Path for Pickle'),
    'data': fields.String(required=True, description='Data for Pickle'),
})

anomalyDetection = api.model('anomalyDetection', {
    'timestamp': fields.String(required=True, description='Timestamp for anomaly detection'),
    'objective_field': fields.String(required=True, description='objective_field for anomaly detection'),
    'file_path': fields.String(required=True, description='file_path for anomaly detection'),
})

forecasting = api.model('forecasting', {
    'periods': fields.Integer(required=True, description='Periods for forecasting'),
    'file_path': fields.String(required=True, description='File Path for forecasting'),
})


