import json
import os

from flask import jsonify


class APIResponseConfig(json.JSONEncoder):

    def __init__(self, code, message):
        self.code = code
        self.message = message

    @classmethod
    def TakeJsonResponse(cls, code, description, result={}):
        try:
            switcher = {
                1: {
                    'code': '01',
                    'status': 'success',
                    'result': result,
                    'description': description,
                    'httpStatus': '200'
                },
                2: {
                    'code': '02',
                    'status': 'validation-error',
                    'result': result,
                    'description': description,
                    'httpStatus': '400'
                },
                3: {
                    'code': '03',
                    'status': 'error',
                    'result': result,
                    'description': 'Bad Request',
                    'httpStatus': '400'
                },
                4: {
                    'code': '04',
                    'status': 'error',
                    'result': result,
                    'description': description,
                    'httpStatus': '400'
                },
                5: {
                    'code': '05',
                    'status': 'success',
                    'result': result,
                    'description': description,
                    'httpStatus': '200'
                },
                6: {
                    'code': '06',
                    'status': 'error',
                    'result': result,
                    'description': 'resource not found (Data M Powered)',
                    'httpStatus': '404'
                },
            }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e)
            }
        return jsonify(switcher.get(code, "nothing"))

    @classmethod
    def getAPICallUrl(cls, path):
        base_url = os.getenv('BASE_URL')
        return base_url + path
