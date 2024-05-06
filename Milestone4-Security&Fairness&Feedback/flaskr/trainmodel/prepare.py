# Glenn Xu

import pandas as pd
import joblib
from scipy.spatial.distance import hamming

import pandas as pd
import joblib
from tqdm import tqdm
import os
current_directory = os.getcwd()

def get_user_info(user_id, raise_for_status = False):
    api_url = f"http://128.2.204.215:8080/user/{user_id}"
    
    try:
        response = requests.get(api_url)
        if raise_for_status:
            response.raise_for_status()
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



def obtain_prerequisites():
    model_path = 'flaskr/data/model/model.joblib'
    df_path = 'flaskr/data/training_set/rate_log.csv'
    user_info_df_path = 'flaskr/data/training_set/user.csv'
    model = joblib.load(model_path)
    df = pd.read_csv(df_path)
    user_info_df = pd.read_csv(user_info_df_path)
    return model, df, user_info_df

def find_similar_users(new_user_info, user_info_df):
    distances = user_info_df.apply(lambda x: hamming([x['age'], x['occupation'], x['gender']],
                                                      [new_user_info['age'], new_user_info['occupation'], new_user_info['gender']]), axis=1)
    closest_user_id = distances.idxmin()
    return user_info_df.loc[closest_user_id]['user_id']


def recommend_movies(user_id, model, df, user_info_df, top_n=20):
    user_id = int(user_id)
    if (user_id not in list(user_info_df['user_id'])):
        user_id = find_similar_users(get_user_info(user_id), user_info_df)

    all_movies_set = set(df['movie_id'].unique())
    predictions = [(movie_id, model.predict(user_id, movie_id).est) for movie_id in all_movies_set]
    recommendations = sorted(predictions, key=lambda x: x[1], reverse=True)[:top_n]
    return [item[0] for item in recommendations]


def generate_recommendations_for_all_training_users(model, df, user_info_df):
    users = df['user_id'].unique()
    recommendations_path = 'flaskr/data/model/recommendations_knn.csv'

    with open(recommendations_path, 'w') as file:
        file.write('user_id\trecommended_movies\n')
        
        for user_id in tqdm(users):
            recommended_movies = recommend_movies(user_id, model, df, user_info_df)
            file.write(f'{user_id}\t{recommended_movies}\n')

def prepare_model():
    model, df, user_info_df = obtain_prerequisites()
    generate_recommendations_for_all_training_users(model, df, user_info_df)

if __name__ == "__main__":
    
    # Run only for data preparation and model testing, commented out for accurate code coverage results
    model, df, user_info_df = obtain_prerequisites()
    generate_recommendations_for_all_training_users(model, df, user_info_df)
    