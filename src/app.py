# src/app.py
import pyfiglet
from flask import Flask, Blueprint
from flask_cors import CORS

from .config import app_config
from .models import bcrypt, mongo
from .shared.restplus import api
from .views.RealTimeMLView import ns as realtimeml_namespace


def create_app(env_name):
    """
        Create app
    """
    # app initialization
    app = Flask(__name__)
    CORS(app)
    app.config.from_object(app_config[env_name])
    bcrypt.init_app(app)

    try:
        mongo.init_app(app)
    except Exception as e:
        print("Exception seen: " + str(e))

    blueprint = Blueprint('api', __name__, url_prefix='/api')
    api.init_app(blueprint)
    api.add_namespace(realtimeml_namespace)
    app.register_blueprint(blueprint)

    print(pyfiglet.figlet_format("BIONS ML"))

    @app.route('/', methods=['GET'])
    def index():
        """
    trigger first run of URL
    """
        return pyfiglet.figlet_format("Congratulations ! BIONS", font="digital")

    return app
