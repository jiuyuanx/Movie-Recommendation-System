from flask import Flask, jsonify
from flaskr.prediction.predict import *
import joblib
import pandas as pd

def create_app():
    app = Flask(__name__, instance_relative_config=True)

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

# if __name__ == '__main__':
#     app.run(host="0.0.0.0", debug=True, port=8082, threaded=True)
