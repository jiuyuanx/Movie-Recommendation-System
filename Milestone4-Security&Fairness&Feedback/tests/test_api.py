import numpy as np
from flaskr.trainingdata.fetch_from_API import *
import random
import os 
import pandas as pd
import string

current_directory = os.getcwd()
# df_path = current_directory + '/flaskr/data/training_set/rate_log.csv'
# df = pd.read_csv(df_path)
df = pd.read_csv('flaskr/data/training_set/rate_log.csv')

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

def test_find_valid_movies():
    try:
        movie_ids = df['movie_id']
        assert movie_ids is not None
        subset_len = 500
        movie_chunks = chunks(movie_ids, subset_len)
        selected_movie_chunk = random.choice(movie_chunks)
        results = []
        for movie_title in selected_movie_chunk:
            results.append(get_movie_info(movie_title))
        assert results is not None and np.array(results).shape == (subset_len,)
    except:
        raise Exception('Cannot print movie information')

def test_find_valid_users():
    try:
        user_ids = df['user_id']
        assert user_ids is not None
        subset_len = 500
        user_chunks = chunks(user_ids, subset_len)
        selected_user_chunk = random.choice(user_chunks)
        results = []
        for user_id in selected_user_chunk:
            results.append(get_user_info(user_id))
        assert results is not None and np.array(results).shape == (subset_len,)
    except:
        raise Exception('Cannot print user information')

def test_find_invalid_users():
    try:
        user_ids = [get_random_string(10), get_random_string(20), '0', -1]
        results = []
        for user_id in user_ids:
            results.append(get_user_info(user_id) is not None)
        assert results is not None and any(results) != True
    except:
        raise Exception('Printed user information of users that do not exist')

def test_find_invalid_movies():
    try:
        movie_ids = [get_random_string(10), get_random_string(20), 'the+conversation+1978','spirited+away+2009', '0']
        results = []

        for movie_title in movie_ids:
            results.append(get_movie_info(movie_title) is not None)
        assert results is not None and any(results) != True
    except:
        raise Exception('Printed movie information of movies that dont exist')

def test_movie_api_exception_flag():
    try:
        get_movie_info(get_random_string(8), True)
        raise Exception('Movie API did not trigger Response Exception')
    except:
        print('success')
        assert True
    

def test_user_api_exception_flag():
    try:
        get_user_info(get_random_string(8), True)
        raise Exception('User API did not trigger Response Exception')
    except:
        assert True

