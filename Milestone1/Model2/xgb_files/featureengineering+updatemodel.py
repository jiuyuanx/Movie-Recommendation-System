# -*- coding: utf-8 -*-
"""FeatureEngineering+UpdateModel.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1dT0mABFDOjF12-Nf2n450n1ftCnVgnqO
"""

!pip install pympler -q

import seaborn as sns
import random
import numpy as np
import pandas as pd
import random
import numpy as np
from tqdm.auto import tqdm
import os
import time
import datetime

start = time.time()
save_path = '/content/drive/MyDrive/movie_rec/'
load_path = '/content/drive/MyDrive/movie_rec/'
df_user = pd.read_csv(load_path+"users.csv")
df_u = pd.get_dummies(df_user)
df_movie = pd.read_csv(load_path+"movies.csv")

"""# **Feature Engineering Movie**"""

def movie_feature(dff): #turn list of dictionary to list of strings with only the name
    import ast
    df = dff
    df = df.rename(columns={'id':'movie_id'})
    df['genres'] = dff['genres'].apply( lambda x: [i['name'] for i in ast.literal_eval(x)] )
    df['belongs_to_collection'] = dff['belongs_to_collection'].apply(lambda x:1 if x is not None else 0)
    df['production_countries'] = dff['production_countries'].apply(lambda x: 1 if 'United States of America' in x else 0)
    df['spoken_languages'] =     dff['spoken_languages'].apply(lambda x:1 if 'English' in x else 0)
    df['original_language'] = df['original_language'].astype(str)
    return df[df.columns.difference(['homepage','poster_path', 'tmdb_id', 'original_title','production_companies' ])]

def movie_data_fillna(df_movie_fea):
  df_movie_fea['status'] = df_movie_fea['status'].fillna('Released')
  pd.to_datetime(df_movie_fea['release_date']).mean()
  df_movie_fea['release_date'] = df_movie_fea['release_date'].fillna('2000-01-01')
  df_movie_fea['overview'] = df_movie_fea['overview'].fillna('None')
  df_movie_fea['imdb_id'] = df_movie_fea['imdb_id'].fillna('None')
  df_movie_fea[pd.isna(df_movie_fea['original_language'])]['original_language']='en'
  df_movie_fea['original_language'] = df_movie_fea['original_language'].apply(lambda x:x if type(x)==list else [x])
  df_movie_fea.isnull().sum()
  return df_movie_fea

def expand_list_to_rows(df, column_name): #expand columns with list of multiple values to multiple rows
    """
    Expand each list in the specified column of the dataframe into separate rows, while retaining the original index.
    """
    rows = []
    counter =0
    for i, row in df.iterrows():
        lists = row[column_name]
        if type(lists)==list:
          if lists != 'None' and len(lists)!=0:
              for item in lists:
                  new_row = row.to_dict()
                  new_row[column_name] = item
                  new_row['original_index'] = i  # Add original index to each row
                  rows.append(new_row)
          else:
              new_row = row.to_dict()
              new_row[column_name] = 'None'
              new_row['original_index'] = i
              rows.append(new_row)
    expanded_df = pd.DataFrame(rows)
    return expanded_df

def efficient_movie_feature_encoding(df, list_columns):
    """
    Efficiently one-hot encode columns that contain lists as values.
    """
    # First, handle normal features that don't need expansion
    useless_feature = ['imdb_id', 'overview', 'title', 'tmdb_id']
    df = df[df.columns.difference(useless_feature)]
    normal_features_df = df[df.columns.difference(list_columns + useless_feature)]

    # Then, for each list column, expand and one-hot encode it
    encoded_dfs = [normal_features_df]
    for column in tqdm(list_columns):
        expanded_df = expand_list_to_rows(df, column)
        encoded_df = pd.get_dummies(expanded_df, columns=[column], prefix=column)
        encoded_df = encoded_df[encoded_df.columns.difference(list_columns)]

        reduced_df = encoded_df.groupby('original_index').last()
        # Aggregate back to original DataFrame index
        encoded_dfs.append(reduced_df)

    final_df = pd.concat(encoded_dfs, axis=1)

    # Handle any duplicated columns due to concatenation, if necessary
    final_df = final_df.loc[:,~final_df.columns.duplicated()]

    return final_df

def movie_final_clean(df_m):
  df_m = df_m.drop(columns=['production_countries'])
  df_m['release_date'] = pd.to_datetime(df_m['release_date']).dt.year
  df_m['adult'] = df_m['adult'].astype(int)
  return df_m

def movie_feature_enigneering(df_movie):
  global df_movie_fea
  df_movie_fea = movie_feature(df_movie)
  df_movie_fea = movie_data_fillna(df_movie_fea)
  df_m =         efficient_movie_feature_encoding(df_movie_fea, ['genres','original_language'])
  df_m =         movie_final_clean(df_m)
  return df_m

df_m = movie_feature_enigneering(df_movie)
df_m.to_csv(save_path+"movie_all_features.csv")
df_m

"""# **Feature Engineering User**"""

df_rating = pd.read_csv(load_path+"rate_log.csv").rename(columns={'minutes':'rating'})
df_watch = pd.read_csv(load_path+"watch_log.csv").rename(columns={'start_time':'time'})

df_full = df_rating.merge(df_u, 'left', on='user_id').fillna(0)
df_full = df_full.merge(df_movie_fea, 'left', left_on='movie_id', right_on='movie_id')
df_full = df_full.merge(df_watch[['user_id', 'movie_id', 'minutes']], 'left', on=['user_id', 'movie_id'], suffixes=('', '_watch')).fillna(0)

total_len = len(df_rating)
indices = list(range(total_len))
random.seed(42)
random.shuffle(indices)
split_point = int(0.75 * total_len)

training_indices = indices[:split_point]
test_indices = indices[split_point:]

data_train = df_full.iloc[training_indices]
data_test = df_full.iloc[test_indices]

user_watch_genre = df_full[['user_id', 'minutes','genres']]
user_watch_genre['genres'] = user_watch_genre['genres'].apply(lambda x:x[0] if (type(x)==list and len(x)>0) else 'None')
user_watch_genre =user_watch_genre.groupby(['user_id', 'genres'])['minutes'].mean().unstack(fill_value=0).reset_index()

user_rate_genre = df_full[['user_id', 'rating','genres']]
user_rate_genre['genres'] = user_rate_genre['genres'].apply(lambda x:x[0] if (type(x)==list and len(x)>0) else 'None')
user_rate_genre = user_rate_genre.groupby(['user_id', 'genres'])['rating'].count().unstack(fill_value=0).reset_index()

df_u_fea=df_u
df_u_fea = df_u_fea.merge(user_rate_genre, 'left', 'user_id', suffixes=('', '_rate')).fillna(0)
df_uu = df_u_fea.merge(user_watch_genre, 'left', 'user_id', suffixes=('_rate','_watch')).fillna(0)

df_uu.to_csv(save_path+'user_all_features.csv')
# df_mm = df_m[~df_m['id'].isin(['becoming+chaz+2011', 'the+butter+battle+book+1989', 'la+pointe-courte+1955', 'shoot+first_+die+later+1974'])]

data = df_rating.merge(df_uu, 'left', on='user_id').fillna(0)
data = data.merge(df_m, 'left', on='movie_id').fillna(0)

from sklearn.preprocessing import OrdinalEncoder
user_encoder = OrdinalEncoder()
user_encoder.fit(data['user_id'].values.reshape(-1, 1))
data['uid'] = user_encoder.transform(data['user_id'].values.reshape(-1, 1)).astype(int)

movie_encoder = OrdinalEncoder()
movie_encoder.fit(np.array(list(set(data['movie_id'].values).union(set(df_m['movie_id'].values)))).reshape(-1, 1))
data['mid'] = movie_encoder.transform(data['movie_id'].values.reshape(-1, 1)).astype(int)

import pickle
uid_user = [dict(enumerate(mapping)) for mapping in user_encoder.categories_][0]
mid_movie = [dict(enumerate(mapping)) for mapping in movie_encoder.categories_][0]
movie_mid = {v: k for k, v in mid_movie.items()}
user_uid = {v: k for k, v in uid_user.items()}
with open(load_path+'uid_user.pkl', 'wb') as f:
    pickle.dump(uid_user, f)
with open(load_path+'mid_movie.pkl', 'wb') as f:
    pickle.dump(mid_movie, f)
with open(load_path+'movie_mid.pkl', 'wb') as f:
    pickle.dump(movie_mid, f)
with open(load_path+'user_uid.pkl', 'wb') as f:
    pickle.dump(user_uid, f)
# movie_mapping

def date_encoding(data):
  data['day_of_week'] = pd.to_datetime(data['time']).dt.dayofweek.astype(int)
  data['is_weekend'] = (data['day_of_week'] >= 5).astype(int)
  data['hour'] = pd.to_datetime(data['time']).dt.hour.astype(int)
  data['hour_sin'] = np.sin(data['hour'] * (2. * np.pi / 24))
  data['hour_cos'] = np.cos(data['hour'] * (2. * np.pi / 24))
  data['day_of_week_sin'] = np.sin(data['day_of_week'] * (2. * np.pi / 7))
  data['day_of_week_cos'] = np.cos(data['day_of_week'] * (2. * np.pi / 7))
  data['release_date'] = pd.to_datetime(data['release_date']).dt.year
  return data.drop(columns=['time'])
data = date_encoding(data)
with pd.option_context('display.max_rows', 10, 'display.max_columns', None):
 display(data)

data['train'] = False
data.loc[training_indices,'train']=True
# data.to_csv(save_path+"train_test_data.csv")
import os
file_stats = os.stat(save_path+"train_test_data.csv")
print(file_stats.st_size / (1024 * 1024), "MB")

def find_non_numerical_columns(df):
    non_numerical_columns = []
    for column in df.columns:
        # Attempt to convert column to numeric type. Unconvertible values are set to NaN
        temp = pd.to_numeric(df[column], errors='coerce')
        # If there are any NaN values in 'temp', it means the conversion was not fully possible
        if temp.isnull().any():
            non_numerical_columns.append(column)
    return non_numerical_columns
find_non_numerical_columns(data)

# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

data_train = data[data['train']==True]
data_test = data[data['train']==False]

X_train = data_train[data_train.columns.difference(['movie_id','uid','user_id', 'status', 'rating', 'train'])]
y_train = data_train['rating']
X_test = data_test[data_test.columns.difference(['movie_id','uid','user_id', 'status', 'rating', 'train'])]
y_test = data_test['rating']


xgb_model = XGBRegressor()#max_depth=5, n_estimators=200)
xgb_model.fit(X_train, y_train)

from sklearn.metrics import mean_squared_error, mean_absolute_error
train_pred =  xgb_model.predict(X_train)
mse = mean_squared_error(y_train,train_pred )
rmse = mse**0.5
mae = mean_absolute_error(y_train,train_pred )

print("Train metrics:")
print(f"MSE: {mse}")
print(f"RMSE: {rmse}")
print(f"MAE: {mae}")
print()

xgb_pred =  xgb_model.predict(X_test)
mse = mean_squared_error(y_test,xgb_pred)
rmse = mse**0.5
mae = mean_absolute_error(y_test,xgb_pred)

print("Test metrics:")
print(f"MSE: {mse}")
print(f"RMSE: {rmse}")
print(f"MAE: {mae}")

xgb_features = pd.DataFrame(columns=list(X_train.columns))
print("#XGBoost features:", len(list(X_train.columns)))
xgb_features.to_csv(save_path+"xgb_features.csv")

import time
X_full = pd.concat([X_train, X_test])
y_full = pd.concat([y_train, y_test])
xgb_model = XGBRegressor()
start1 = time.time()
xgb_model.fit(X_full, y_full)
print('XGBoost Training time:', time.time()-start1, "Seconds")


xgb_model.save_model(save_path+'xgb_model.json')


from pympler import asizeof
import sys

file_stats = os.stat(save_path+"xgb_model.json")
print("XGBoost Model size on disk:", file_stats.st_size / (1024 * 1024), "MB")
approx_model_size = sys.getsizeof(xgb_model)
print("XGBoost approximate  Model size in RAM:", approx_model_size / (1024 * 1024), "MB")
model_size_bytes = asizeof.asizeof(xgb_model)
print(f"XGBoost more accurate Model size in RAM: {model_size_bytes/ (1024 * 1024)} MB")

print('Update time:', time.time()-start, "Seconds")

print("Training RAM cost:", round(5.4-0.8,2), "GB")

