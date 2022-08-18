import os
import time
from flask import Flask, send_from_directory, jsonify, request
import airindex

app = Flask(__name__, static_folder='../templates', static_url_path='')

@app.route("/<path:path>")
def main():
    return send_from_directory(os.path.join('..', 'templates'), path)

@app.route("/dataset", methods=['GET'])
def get_dataset():
    return jsonify(airindex.get_dataset())

@app.route("/tune", methods=['POST'])
def tune_diagram():
    time.sleep(0.3)
    # Currently need to have 3 layers for it to work properly
    param = request.json
    # data = {
    #     "func": param['func'],
    #     "delta": param['delta'],
    #     "data": ["336 B",
    #             "28.6 KB",
    #             "6.5 MB",
    #             "1.6 GB"],
    # }
    return jsonify(airindex.tune_data(param))
