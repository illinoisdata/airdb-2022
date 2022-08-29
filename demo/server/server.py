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

@app.route("/diyCustom", methods=['POST'])
def tune_diy_custom():
    time.sleep(0.3)
    # Currently need to have 3 layers for it to work properly
    param = request.json
    return jsonify(airindex.tune_diy_custom(param["dataset"], param["affine"], param["latency"], param["bandwidth"], param["func"], param["delta"]))

@app.route("/diyBTree", methods=['POST'])
def tune_diy_btree():
    time.sleep(0.3)
    param = request.json
    return jsonify(airindex.tune_diy_btree(param["dataset"], param["affine"], param["latency"], param["bandwidth"]))


@app.route("/airindex", methods=['POST'])
def tune_airindex():
    time.sleep(0.3)
    param = request.json
    return jsonify(airindex.tune_airindex(param["dataset"], param["affine"], param["latency"], param["bandwidth"]))
