from flask import Flask
from flask_cors import CORS
from randomizer.route.auth import auth

app = Flask(__name__)
CORS(app)

app.register_blueprint(auth)
