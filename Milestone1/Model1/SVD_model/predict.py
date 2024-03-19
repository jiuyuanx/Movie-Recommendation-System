# Glenn Xu

import pandas as pd
from surprise import Reader, Dataset
from surprise import SVD, model_selection
import joblib
from scipy.spatial.distance import hamming

def read_user_info(user_info_path):
    return pd.read_csv(user_info_path)

def find_similar_users(new_user_info, user_info_df):
    distances = user_info_df.apply(lambda x: hamming([x['age'], x['occupation'], x['gender']],
                                                      [new_user_info['age'], new_user_info['occupation'], new_user_info['gender']]), axis=1)
    closest_user_id = distances.idxmin()
    return user_info_df.loc[closest_user_id]['user_id']

def recommend_movies(user_id, model, df, user_info_df, top_n=20):


    # For new users
    if user_id not in df['user_id'].unique():
        # user_info_df = read_user_info(user_info_path)
        if user_id in user_info_df['user_id'].values:
            new_user_info = user_info_df[user_info_df['user_id'] == user_id].iloc[0]
            similar_user_id = find_similar_users(new_user_info, user_info_df)
            user_id = similar_user_id
        else:
            print("New user ID not found in user info data.")
            return []

    # For old users
    user_movies = df[df['user_id'] == user_id]['movie_id'].tolist()

    all_movies = df['movie_id'].unique()
    
    predictions = []
    for movie_id in all_movies:
        if movie_id not in user_movies:
            predictions.append((movie_id, model.predict(user_id, movie_id).est))

    recommendations = sorted(predictions, key=lambda x: x[1], reverse=True)[:top_n]
    result = [item[0] for item in recommendations]
    
    return result
