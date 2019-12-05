# src/models/__init__.py

from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_pymongo import PyMongo

# initialize our db
db = SQLAlchemy()
bcrypt = Bcrypt()
mongo = PyMongo()

# from .ProjectModel import ProjectModel
