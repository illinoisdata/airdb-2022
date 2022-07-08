import os
from flask import Flask, send_from_directory

app = Flask(__name__, static_folder='../templates', static_url_path='')

@app.route("/<path:path>")
def main():
    return send_from_directory(os.path.join('..', 'templates'), path)
