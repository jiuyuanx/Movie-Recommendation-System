# Glenn Xu

import pandas as pd
from surprise import Reader, Dataset
from surprise import SVD, model_selection
import joblib
import sys
from pympler import asizeof
import os



def adjust_path(suffix):
    current_directory = os.getcwd()
    end_of_path_name = current_directory.split('/')[-1]
    path = suffix if end_of_path_name == "/../_SHARED_DATA_POOL" else  current_directory + '/../_SHARED_DATA_POOL/' + suffix
    return path

def read_dataloader():
    # path = adjust_path('training_set/rate_log.csv')
    path = 'flaskr/data/training_set/rate_log.csv'
    df = pd.read_csv(path)
    return df

def cast_dataloader(df):
    reader = Reader(rating_scale=(0, 5))
    data = Dataset.load_from_df(df[['user_id', 'movie_id', 'rate']], reader)
    #add assert statement
    return data

def cross_validate_model(data, measures = ['RMSE', 'MAE'], cv = 5, verbose = True):
    model = SVD()
    scores = model_selection.cross_validate(model, data, measures=measures, cv=cv, verbose=verbose)
    return model, scores

def fit_model(data, model):
    trainset = data.build_full_trainset()
    model.fit(trainset)
    
def dump_model(model):
    # model_path = adjust_path('model')
    model_path = 'flaskr/data/model' 
    print(model_path)
    if not os.path.exists(model_path):
        os.mkdir(model_path)
    joblib.dump(model, model_path + '/model.joblib')

def generate_model_and_info():
    df = read_dataloader()
    data = cast_dataloader(df)
    model, _ = cross_validate_model(data)
    fit_model(data, model)
    dump_model(model)
    # model_path = adjust_path('model/svd_model.joblib')
    model_path = adjust_path('model/model.joblib')
    # model_path = 'flaskr/data/model/knn_model.joblib' 
    model_disk_size = os.path.getsize(model_path)
    approx_model_size_in_memory = sys.getsizeof(model)
    accurate_model_size_in_memory = asizeof.asizeof(model)

    print('\n')
    print("Model Size on Disk: ", model_disk_size)
    print("Approximate Model Size in Memory (sys.getsizeof): ", approx_model_size_in_memory)
    print("More Accurate Model Size in Memory (pympler.asizeof): ", accurate_model_size_in_memory)
    return model_disk_size, approx_model_size_in_memory, accurate_model_size_in_memory

def train_model():
    generate_model_and_info()

if __name__ == "__main__":
    generate_model_and_info()

    

   