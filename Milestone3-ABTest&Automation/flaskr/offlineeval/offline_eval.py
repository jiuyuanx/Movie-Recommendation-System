import pandas as pd
from surprise import Reader, Dataset, accuracy
from surprise import KNNBasic, model_selection
import re

def load_df():
    df = pd.read_csv('flaskr/offlineeval/data/rate_log_ts.csv', index_col=0)
    df = df.rename(columns={'user_id': 'user', 'movie_name_id': 'item', 'movie_score': 'rating'})
    return df


def process_ts(ts):
    pattern = r'[^\dT\-:]'
    ts = re.sub(pattern, '', ts)
    if len(ts) == 16: # 2024-03-04T05:31 -> no seconds
        ts += ':00'
    return ts


def preprocess_data(df):
    df['timestamp'] = df['timestamp'].map(process_ts)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df = df.dropna()
    return df

def fit_and_compute_predictions():
    df = load_df()
    df = preprocess_data(df)

    df.to_csv('flaskr/offlineeval/data/processed_ratings.csv', index=False)

    reader = Reader(line_format="timestamp user item rating", sep=',', skip_lines=1)
    data = Dataset.load_from_file('flaskr/offlineeval/data/processed_ratings.csv', reader)

    trainset, testset = model_selection.train_test_split(data, shuffle=False)
    print(f"Train Size: {trainset.n_users}\tTest Size: {len(testset)}")

    model = KNNBasic()
    model.fit(trainset)
    pred = model.test(testset)
    return pred

def obtain_model_statistics(pred):
    rmse = accuracy.rmse(pred)
    mae = accuracy.mae(pred)

    print(f"RMSE: {rmse} MAE: {mae}")
    rmse = accuracy.rmse(pred)

    return rmse, mae




if __name__ == '__main__':
    pred = fit_and_compute_predictions()
    obtain_model_statistics(pred)
    

    