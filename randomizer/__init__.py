from flask import Flask
from flask_cors import CORS
from randomizer.routes.route import route
from randomizer.routes.auth import auth

app = Flask(__name__)
CORS(app)

app.register_blueprint(route)
app.register_blueprint(auth)