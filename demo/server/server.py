import os
import time
from flask import Flask, send_from_directory, jsonify

app = Flask(__name__, static_folder='../templates', static_url_path='')

@app.route("/<path:path>")
def main():
    return send_from_directory(os.path.join('..', 'templates'), path)

@app.route("/profile", methods=['GET'])
def get_profile():
    data = [1.5, 1.5, 2, 3]
    return jsonify(data)

@app.route("/diagram", methods=['GET'])
def get_diagram():
    time.sleep(0.3)
    data = [
        {
        "text": "piecewise linear, 336 B",
        "color": "orange",
        "line": "line"
        },
        {
        "text": "piecewise step, 28.6 KB",
        "color": "black",
        "line": "arrow"
        },
        {
        "text": "data layer, 1.6 GB",
        "color": "black",
        "line": "none"
        }
    ]
    return jsonify(data)
