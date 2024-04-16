from flask import Flask, jsonify
from helper_modules.predict import *
import joblib
import pandas as pd
import requests
import random
import csv
from datetime import datetime
app = Flask(__name__)

model = joblib.load('/home/team25/Milestone2/_SHARED_DATA_POOL/model/svd_model.joblib')
df = pd.read_csv('/home/team25/Milestone2/_SHARED_DATA_POOL/training_set/rate_log.csv')
user_info_df = pd.read_csv('/home/team25/Milestone2/_SHARED_DATA_POOL/training_set/user.csv')
prepare_df = pd.read_csv('/home/team25/Milestone2/_SHARED_DATA_POOL/model/recommendations.csv', sep='\t')


@app.route('/recommend/<user_id>', methods=['GET'])
def recommend(user_id):
    try:
        recommendations = recommend_movies(user_id, model, df, user_info_df, prepare_df)
        ordered_comma_separated_ids = ",".join(map(str, recommendations))
        return ordered_comma_separated_ids, 200
    except Exception as e:
        print(e)  
        return jsonify({"message": "An error occurred"}), 500
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=8082, threaded=True)
