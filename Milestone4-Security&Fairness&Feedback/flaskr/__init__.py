from flask import Flask, jsonify
from flaskr.prediction.predict import *
from flaskr.trainingdata.fetch_training_data import fetch_train_data
from flaskr.trainmodel.train import train_model
from flaskr.trainmodel.prepare import prepare_model
import joblib
import pandas as pd
from flask_crontab import Crontab
import gc

crontab = Crontab()
model, df, user_info_df, prepare_df = None, None, None, None
logging = None

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    crontab.init_app(app)
    global logging
    logging = app.logger

    model = joblib.load('flaskr/data/model/model.joblib')
    df = pd.read_csv('flaskr/data/training_set/rate_log.csv')
    user_info_df = pd.read_csv('flaskr/data/training_set/user.csv')
    prepare_df = pd.read_csv('flaskr/data/model/recommendations.csv', sep='\t')

    @app.route('/recommend/<user_id>', methods=['GET'])
    def recommend(user_id):
        try:
            recommendations = recommend_movies(user_id, model, df, user_info_df, prepare_df)
            ordered_comma_separated_ids = ",".join(map(str, recommendations))
            return ordered_comma_separated_ids, 200
        except Exception as e:
            print(e)  
            return jsonify({"message": "An error occurred"}), 500
    
    return app

def reload_model():
    global model, df, user_info_df, prepare_df
    model = joblib.load('flaskr/data/model/svd_model.joblib')
    df = pd.read_csv('flaskr/data/training_set/rate_log.csv')
    user_info_df = pd.read_csv('flaskr/data/training_set/user.csv')
    prepare_df = pd.read_csv('flaskr/data/model/recommendations.csv', sep='\t') 

@crontab.job(minute="*/2")
def update_model():
    print("updating model")
    global logging
    logging.info("Updated model")
    fetch_train_data()
    train_model()
    prepare_model()
    reload_model()
    gc.collect()

# if __name__ == '__main__':
#     app.run(host="0.0.0.0", debug=True, port=8082, threaded=True)
