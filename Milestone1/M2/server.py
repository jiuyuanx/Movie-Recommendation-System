from flask import Flask, jsonify
from xgb_files.xgb_inference import *
import time
import requests
import warnings
from datetime import datetime
# warnings.filterwarnings("ignore")

app = Flask(__name__)

load_path = '/home/team25/M2/xgb_files/'
uid_user, mid_movie, movie_mid, user_uid, xgb_model, df_mm, df_uu, xgb_features  = init(load_path)

df = pd.read_csv('/home/team25/M2/xgb_files/user_all_features.csv')

def get_recommendations_by_xgboost(user_id):

    user_id = int(user_id)
    start=time.time()
    X_inference = inference_set_up(user_id, df_uu, df_mm, user_uid, movie_mid, xgb_features, datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

    rate, ind, rec = inference(user_id,  X_inference, mid_movie, xgb_model, 20)
    print("Inference time:", time.time()-start, "Seconds", user_id)
    return rec

def fetch_user_info(user_id):
    """Fetch user info from the external API."""
    response = requests.get(f'http://128.2.204.215:8080/user/{user_id}')
    if response.status_code == 200:
        return response.json()
    else:
        return None

def find_similar_user(new_user_info, user_info_df):
    """Find a similar user based on the provided user information."""
    # Replace spaces and slashes to match column naming convention

    gender_column = 'gender_F' if new_user_info['gender'] == 'F' else 'gender_M'
    
    # Now proceed with the filtering based on the corrected column names
    filtered_df = user_info_df[(user_info_df[gender_column] == 1)]
    
    if not filtered_df.empty:
        similar_user_id = filtered_df['user_id'].sample(n=1).iloc[0]
    else:
        similar_user_id = user_info_df['user_id'].sample(n=1).iloc[0]

    return similar_user_id

@app.route('/recommend/<user_id>', methods=['GET'])
def recommend(user_id):
    if int(user_id) not in df['user_id'].values:
        new_user_info = fetch_user_info(user_id)
        if new_user_info:
            try:
                similar_user_id = find_similar_user(new_user_info, df)
                user_id = similar_user_id  # Use the similar user's ID for recommendations
            except:
                user_id = 1


    # try:
    recommendations = get_recommendations_by_xgboost(user_id)

    ordered_comma_separated_ids = ",".join(map(str, recommendations))

    return ordered_comma_separated_ids
    # except:
    #     # return fault
    #     return jsonify({"message": "error"}), 500   



if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=8082, threaded=True)

















