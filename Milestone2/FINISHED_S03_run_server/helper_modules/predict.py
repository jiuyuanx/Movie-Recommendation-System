# Glenn Xu

import pandas as pd
from surprise import Reader, Dataset
from surprise import SVD, model_selection
import joblib
from scipy.spatial.distance import hamming
import pandas as pd
from surprise import SVD
import joblib
from tqdm import tqdm
import requests


# Function to get user information by user_id
def get_user_info(user_id):
    api_url = f"http://128.2.204.215:8080/user/{user_id}"
    
    try:
        response = requests.get(api_url)
        # Check if the user was found
        if response.status_code == 200:
            user_info = response.json()
            # Handle the case where the user is not found
            return user_info
        else:
            print(f"Failed to fetch user with ID {user_id}. HTTP Status Code: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"An error occurred while fetching user with ID {user_id}: {e}")
        return None


def read_user_info():
    return pd.read_csv('/home/team25/Milestone2/_SHARED_DATA_POOL/training_set/user.csv')


def find_similar_users(new_user_info, user_info_df):
    distances = user_info_df.apply(lambda x: hamming([x['age'], x['occupation'], x['gender']],
                                                      [new_user_info['age'], new_user_info['occupation'], new_user_info['gender']]), axis=1)
    closest_user_id = distances.idxmin()
    return user_info_df.loc[closest_user_id]['user_id']


def recommend_movies(user_id, model, df, user_info_df, prepare_df, top_n=20):
    user_id = int(user_id)
    if (user_id not in list(user_info_df['user_id'])):
        user_id = find_similar_users(get_user_info(user_id), user_info_df)
    movie_list = prepare_df.loc[prepare_df['user_id'] == user_id, 'recommended_movies'].iloc[0].split('\', \'')
    movie_list[0] = movie_list[0][2:]
    movie_list[-1] = movie_list[-1][:-2]
    return movie_list


# model = joblib.load('/home/team25/Milestone2/_SHARED_DATA_POOL/model/svd_model.joblib')
# df = pd.read_csv('/home/team25/Milestone2/_SHARED_DATA_POOL/training_set/rate_log.csv')
# user_info_df = pd.read_csv('/home/team25/Milestone2/_SHARED_DATA_POOL/training_set/user.csv')
# prepare_df = pd.read_csv('/home/team25/Milestone2/_SHARED_DATA_POOL/model/recommendations.csv', sep='\t')
# user_cache = pd.read_csv('/home/team25/Milestone2/_SHARED_DATA_POOL/cache/user.csv')

# recommend_movies(2, model, df, user_info_df, prepare_df, user_cache)

