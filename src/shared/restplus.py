import logging
import os
import traceback

from flask_restplus import Api
from sqlalchemy.orm.exc import NoResultFound
from ..config import app_config

log = logging.getLogger(__name__)

api = Api(version='1.0', title='BIONS REAL TIME ML',
          description='Be it a start-up or an industry giant, Businesses today must embrace digital transformation in order to secure competitive advantage. Organizations have flourished in these new environments by creating digital and mobile centric business models that are disrupting entire industries.')


@api.errorhandler
def default_error_handler(e):
    message = 'An unhandled exception occurred.'
    log.exception(message)

    if not app_config[os.getenv('FLASK_ENV')].DEBUG:
        return {'message': message}, 500


@api.errorhandler(NoResultFound)
def database_not_found_error_handler(e):
    log.warning(traceback.format_exc())
    return {'message': 'A database result was required but none was found.'}, 404
