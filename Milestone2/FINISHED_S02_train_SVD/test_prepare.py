import prepare
import numpy as np
from helper_modules.fetch_from_API import *
import random
import string

model, df, user_info_df = prepare.obtain_prerequisites()

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def chunks(xs, n):
    n = max(1, n)
    data = []
    for i in range(0, len(xs), n):
        data.append(xs[i:i+n])
    return data

def test_prerequisites_exist():
    try:
        assert model is not None 
        assert df is not None
        assert user_info_df is not None
        assert len(df.shape) == 2 and df.shape == (2000, 3)
        assert len(user_info_df.shape) == 2 and user_info_df.shape == (2001, 4)
    except:
        raise('Error in obtaining prerequisite materials for model prediction')

def test_find_sim_users():
    users_ids = user_info_df['user_id']
    assert users_ids is not None
    random_user_ids = np.random.choice(users_ids, size=10, replace=False)
    result_ids = []
    for random_user_id in random_user_ids:
        result_ids.append(prepare.find_similar_users(get_user_info(random_user_id), user_info_df))
    assert random_user_ids is not None
    assert result_ids is not None
    assert len(random_user_ids) == len(result_ids)

def find_sim_user(_id, user_info_df):
    try:
        res = prepare.find_similar_users(get_user_info(_id), user_info_df)
        return True
    except:
        return False

def test_invalid_find_sim_users():
    try:
        user_ids = user_info_df['user_id']
        assert user_ids is not None
        invalid_user_ids = [None, 0, max(user_ids) * 100, -1*min(user_ids), get_random_string(8), get_random_string(6), get_random_string(4)]
        result_ids = []
        for _id in invalid_user_ids:
            result_ids.append(find_sim_user(_id, user_info_df))
        assert any(result_ids) != True
    except:
        raise('Model is working on invalid inputs - either all or some of them')

def test_recommendations(k = 20):
    try:
        users = df['user_id'].unique()
        new_users = []
        subset_len = 501
        user_chunks = chunks(users, subset_len)
        selected_user_chunk = random.choice(user_chunks)
        
        for user_id in range(1, len(selected_user_chunk)):
            if user_id not in set(users):
                new_users.append(user_id)
        selected_ids = np.random.choice(new_users, subset_len//2, replace = False)
        data = []
        for user_id in selected_ids:
            recommended_movies = prepare.recommend_movies(user_id, model, df, user_info_df, top_n=k)
            data.append(recommended_movies)
        assert data is not None and np.array(data).shape == (len(selected_ids), k)
    except:
        raise('Error in recommending movies to new users')

def test_recommendations_for_all_users():
    try:
        prepare.generate_recommendations_for_all_training_users(model, df, user_info_df)
    except:
        raise('Error in recommending movies for all current users')

def test_user_api_exception_flag():
    try:
        get_user_info(get_random_string(8), True)
        raise Exception('User API did not trigger Response Exception')
    except:
        assert True

