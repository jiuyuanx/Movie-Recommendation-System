import json
import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader
import os
import pickle
from datetime import datetime

# Define the RecommendationInferenceDataset class
class RecommendationInferenceDataset(Dataset):
    def __init__(self, features):
        self.uid = torch.LongTensor(features['uid'].values)
        self.iid = torch.LongTensor(features['mid'].values)
        self.features = torch.FloatTensor(features[features.columns.difference(['uid', 'mid'])].values)
        self.features = (self.features - self.features.mean(-1, keepdim=True)) / self.features.mean(-1, keepdim=True)

    def __len__(self):
        return self.iid.shape[0]

    def __getitem__(self, id):
        return self.uid[id], self.iid[id], self.features[id]

def date_encoding(data):
    data['day_of_week'] = pd.to_datetime(data['time']).dt.dayofweek.astype(int)
    data['is_weekend'] = (data['day_of_week'] >= 5).astype(int)
    data['hour'] = pd.to_datetime(data['time']).dt.hour.astype(int)
    data['hour_sin'] = np.sin(data['hour'] * (2. * np.pi / 24))
    data['hour_cos'] = np.cos(data['hour'] * (2. * np.pi / 24))
    data['day_of_week_sin'] = np.sin(data['day_of_week'] * (2. * np.pi / 7))
    data['day_of_week_cos'] = np.cos(data['day_of_week'] * (2. * np.pi / 7))
    data['release_date'] = pd.to_datetime(data['release_date']).dt.year
    return data

def inference_set_up(user_id, df_user, df_movie, user_mapping, feature_name, time):
    df_merged = df_movie
    df_merged['mid'] = df_merged['id'].apply(lambda x: movie_mid[x])
    df_merged = df_merged.drop(columns=['id'])
    df_merged = pd.get_dummies(df_merged).reset_index(drop=True)

    user_feature = df_user[df_user['user_id']==user_id].reset_index(drop=True).iloc[0:1]

    for column in user_feature.columns:
        df_merged[column] = user_feature[column].values[0]

    uid = user_mapping[user_id]
    df_merged['uid'] = [uid] * len(df_merged)
    df_merged['time'] =[time]* len(df_merged)
    df_merged = date_encoding(df_merged)


    df_merged = pd.concat([df_merged, user_feature], axis=1)
    X_inference = df_merged.loc[:, list(feature_name.columns)]


    return X_inference.sort_values('mid')

def inference(user_id, X_inference, movie_mapping):
    inference_data = RecommendationInferenceDataset(X_inference)

    inference_loader = DataLoader(
        dataset=inference_data,
        batch_size=len(inference_loader),  # Adjust batch size according to your model and GPU capacity
        shuffle=False,
    )
    predictions = []
    with torch.no_grad():  # Ensure to not compute gradients during inference to save memory and speed up
        for uid, iid, x in inference_loader:
            uid, iid, x = uid.to(device), iid.to(device), x.to(device)
            y_pred = model(uid, iid, x).squeeze()
            predictions.append(y_pred.cpu().numpy())

    predictions = np.concatenate(predictions)
    sorted_indices = np.argsort(-predictions)[:10]

    return predictions[sorted_indices], sorted_indices, [movie_mapping[i] for i in sorted_indices]

def init():
    global model, device, uid_user, mid_movie, movie_mid, user_uid, df_mm, df_uu, features
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    
    model_path = os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'model.pt')
    model = torch.jit.load(model_path)
    model.eval()
    model.to(device)
    
    # Load additional required data for preprocessing
    with open(os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'uid_user.pkl'), 'rb') as f:
        uid_user = pickle.load(f)
    with open(os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'mid_movie.pkl'), 'rb') as f:
        mid_movie = pickle.load(f)
    with open(os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'movie_mid.pkl'), 'rb') as f:
        movie_mid = pickle.load(f)
    with open(os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'user_uid.pkl'), 'rb') as f:
        user_uid = pickle.load(f)
    
    # Load your dataframe CSVs into pandas DataFrames
    df_mm_path = os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'movie_all_features.csv')
    df_mm = pd.read_csv(df_mm_path, index_col=0)
    
    df_uu_path = os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'user_all_features.csv')
    df_uu = pd.read_csv(df_uu_path, index_col=0)
    
    features_path = os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'features.csv')
    features = pd.read_csv(features_path, index_col=0)

def run(raw_data):
    try:
        data = json.loads(raw_data)
        user_id = data['user_id']
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Or get from input data if provided
        
        X_inference = inference_set_up(user_id, df_uu, df_mm, user_uid, features, time)
        predictions, sorted_indices, movie_ids = inference(user_id, X_inference, mid_movie)
        
        result = {
            "predictions": predictions.tolist(),
            "sorted_indices": sorted_indices.tolist(),
            "movie_ids": movie_ids
        }
        return json.dumps(result)
    except Exception as e:
        error = {"error": str(e)}
        return json.dumps(error)
