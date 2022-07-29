import os
import time
from flask import Flask, send_from_directory, jsonify

app = Flask(__name__, static_folder='../templates', static_url_path='')

@app.route("/<path:path>")
def main():
    return send_from_directory(os.path.join('..', 'templates'), path)

@app.route("/dataset", methods=['GET'])
def get_dataset():
    dataset = [
        "books_800M_uint64",
        "fb_200M_uint64",
        "osm_cellids_800M_uint64",
        "wiki_ts_200M_uint64"]
    return jsonify(dataset)

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
