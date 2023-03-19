from flask import Flask
from flask_cors import CORS
from randomizer.route import route

app = Flask(__name__)
CORS(app)

app.register_blueprint(route)
