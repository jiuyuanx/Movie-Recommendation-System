import pandas as pd
import mysql.connector
import numpy as np
import time
import joblib
import os
import argparse
from surprise import SVD, Dataset, Reader, accuracy
from surprise.model_selection import train_test_split
from surprise.accuracy import rmse, mae


# def parse_args():
#     parser = argparse.ArgumentParser()

#     # SageMaker container environment variables
#     parser.add_argument('--model', type=str, default=os.environ['SM_MODEL_DIR'])
#     parser.add_argument('--train', type=str, default=os.environ['SM_CHANNEL_TRAIN'])

#     return parser.parse_args()

if __name__ == "__main__":
    # The input data directory (SageMaker makes the S3 data available here)
    train_dir = "./"#os.environ['SM_CHANNEL_TRAIN']
    
    # The model output directory
    model_dir = "./"#os.environ['SM_MODEL_DIR']
    
    # Load dataset
    train_file = os.path.join(train_dir, 'train_data.csv')
    df = pd.read_csv(train_file)
    
    # Setup the reader and dataset
    reader = Reader(rating_scale=(1, 5))
    data = Dataset.load_from_df(df[['userid', 'movieid', 'rating']], reader)
    
    # Split data into training and test set
    trainset, testset = train_test_split(data, test_size=0.25, shuffle=False)
    
    # Train SVD model
    algo = SVD()
    algo.fit(trainset)
    
    # Make predictions on the test set
    predictions = algo.test(testset)
    
    # Calculate and print MAE and RMSE
    mae = accuracy.mae(predictions, verbose=True)
    rmse = accuracy.rmse(predictions, verbose=True)
    
    # Save the trained model
    joblib.dump(algo, os.path.join(model_dir, 'model.joblib'))
    # Additionally, write the performance metrics to a file
    print(f"mae={mae};")
    print(f"rmse={rmse};")
    with open(os.path.join(model_dir, 'performance_metrics.txt'), 'w') as f:
        f.write(f"MAE: {mae}\n")
        f.write(f"RMSE: {rmse}\n")
        

