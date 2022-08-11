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

@app.route("/tune", methods=['GET'])
def tune_diagram():
    time.sleep(0.3)
    # Currently need to have 3 layers for it to work properly
    data = [
        "336 B",
        "28.6 KB",
        "6.5 MB",
        "1.6 GB",
    ]
    return jsonify(data)
