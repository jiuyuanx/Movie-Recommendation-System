# Glenn Xu

import pandas as pd
from surprise import Reader, Dataset
from surprise import SVD, model_selection
import joblib
from scipy.spatial.distance import hamming
import sys
from pympler import asizeof
import os

df = pd.read_csv(r'./data/rate_log.csv')
print(df.head())

reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(df[['user_id', 'movie_id', 'rate']], reader)

model = SVD()

model_selection.cross_validate(model, data, measures=['RMSE', 'MAE'], cv=5, verbose=True)

trainset = data.build_full_trainset()
model.fit(trainset)

joblib.dump(model, './model/svd_model.joblib')

model_disk_size = os.path.getsize('./model/svd_model.joblib')
approx_model_size_in_memory = sys.getsizeof(model)
accurate_model_size_in_memory = asizeof.asizeof(model)

print('\n')
print("Model Size on Disk: ", model_disk_size)
print("Approximate Model Size in Memory (sys.getsizeof): ", approx_model_size_in_memory)
print("More Accurate Model Size in Memory (pympler.asizeof): ", accurate_model_size_in_memory)