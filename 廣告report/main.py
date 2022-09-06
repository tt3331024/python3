# coding: utf-8
""" 廣告成效周報 。整理各家代理商的成效報告。並整合後再按邏輯計算每日成效。

Version 1.0.0
Created on 2021/12/29 by Jerry.Ko
"""

import os
# import datetime
# import numpy as np
# import pandas as pd
# from dateutil.parser import parse
from preprocess_data import preprocess_data
from excel_format import set_style_for_overview, set_style_for_performance
from werkzeug.utils import secure_filename
from flask import Flask, request, redirect, url_for, render_template, send_from_directory

UPLOAD_FOLDER = './tmp'
ALLOWED_EXTENSIONS = set(['xlsx'])

#### used functions END ####
#### flask START ####
app = Flask(__name__, template_folder='./templates')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 8 * 1024 * 1024   #8MB

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

def remove_all_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            os.remove(file_path)

@app.route('/')
def index():
    remove_all_files(app.config['UPLOAD_FOLDER'])
    return render_template('index.html')#, template_folder='./')

@app.route('/upload', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        uploaded_files = request.files.getlist("file[]")
        filenames = []
    for file in uploaded_files:
        if file and allowed_file(file.filename):
            filename = file.filename.replace("../", '')
#             print("filename: {}".format(file.filename))
#             filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            filenames.append(filename)
    return download_file(app.config['UPLOAD_FOLDER'])


# @app.route('/upload/download')
def download_file(dir_path):
    preprocess_data(dir_path)
    set_style_for_overview(os.path.join(dir_path, 'output.xlsx'))
    set_style_for_performance(os.path.join(dir_path, 'output.xlsx'))
    # set_excle_style(os.path.join(app.config['UPLOAD_FOLDER'], 'output.xlsx'))
    return send_from_directory(dir_path, 'output.xlsx', as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080) # for docker
    # app.run(host='127.0.0.1', port=8080)
