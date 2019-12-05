# /src/config.py

import os
from dotenv import load_dotenv

load_dotenv()
# OR, the same with increased verbosity:
load_dotenv(verbose=True)

# OR, explicitly providing path to '.env'
from pathlib import Path  # python3 only

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)


class Development(object):
    """
    Development environment configuration
    """
    DEBUG = True
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY')
    MONGO_URI = os.getenv('DATABASE_URL')


class Production(object):
    """
    Production environment configurations
    """
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY')
    MONGO_URI = "mongodb://localhost:27017/roytuts"


class Testing(object):
    """
    Development environment configuration
    """
    TESTING = True
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    MONGO_URI = "mongodb://localhost:27017/roytuts"


app_config = {
    'development': Development,
    'production': Production,
    'testing': Testing
}
