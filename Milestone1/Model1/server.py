from flask import Flask, jsonify
from SVD_model.predict import recommend_movies
import joblib
import pandas as pd
import requests

app = Flask(__name__)

model = joblib.load('/home/team25/M1/SVD_model/model/svd_model.joblib')
df = pd.read_csv('/home/team25/M1/SVD_model/data/rate_log.csv')
user_info_df = pd.read_csv('/home/team25/M1/SVD_model/data/users.csv')

def fetch_user_info(user_id):
    """Fetch user info from the external API."""
    response = requests.get(f'http://128.2.204.215:8080/user/{user_id}')
    if response.status_code == 200:
        return response.json()
    else:
        return None


def find_similar_user(new_user_info, user_info_df):
    """Find a similar user based on the provided user information."""
    # Convert age to integer for comparison
    new_user_age = int(new_user_info['age'])
    new_user_occupation = new_user_info['occupation']
    new_user_gender = new_user_info['gender']
    # Filter users with the same occupation and gender
    similar_users = user_info_df[
        (user_info_df['occupation'] == new_user_occupation) & 
        (user_info_df['gender'] == new_user_gender)
    ]
    # If no users match on occupation and gender, broaden the search to just occupation
    if similar_users.empty:
        similar_users = user_info_df[user_info_df['occupation'] == new_user_occupation]
    # If still no matches, broaden the search to just gender
    if similar_users.empty:
        similar_users = user_info_df[user_info_df['gender'] == new_user_gender]
    # If there are still no matches, return any user
    if similar_users.empty:
        return user_info_df['user_id'].sample(n=1).iloc[0]
    # Find the user with the closest age to the new user
    similar_users['age_difference'] = similar_users['age'].apply(lambda x: abs(x - new_user_age))
    similar_user_id = similar_users.sort_values(by='age_difference').iloc[0]['user_id']

    return similar_user_id


# Dummy function to simulate model predictions. Replace with your actual model.
def get_recommendations(user_id):
    if int(user_id) not in user_info_df['user_id'].values:
        new_user_info = fetch_user_info(user_id)
        if new_user_info:
            similar_user_id = find_similar_user(new_user_info, user_info_df)
            user_id = similar_user_id  # Use the similar user's ID for recommendations
        else:
            # Handle case where user info could not be fetched
            user_id = 1
    return recommend_movies(int(user_id), model, df, user_info_df, top_n=20)



@app.route('/recommend/<user_id>', methods=['GET'])
def recommend(user_id):
    # Get recommendations for the given user ID
    try:
        recommendations = get_recommendations(user_id)
        ordered_comma_separated_ids = ",".join(map(str, recommendations))
        # Return the recommendations as a comma-separated string
        return ordered_comma_separated_ids
    except:
        # return fault
        print("fault")
        return jsonify({"message": "error"}), 500   

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=8082, threaded=True)
