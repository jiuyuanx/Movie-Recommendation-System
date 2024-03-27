#%%
import pandas as pd
import mysql.connector
import numpy as np
import time
import joblib
import os
import argparse


# Import Surprise and other necessary libraries
# We'll attempt to import, and if it fails, we'll install it and then import
import sagemaker
from sagemaker.estimator import Estimator
import boto3
import os

#%%
def train_model_sagemaker():
# Set up the SageMaker session and role
    sagemaker_session = sagemaker.Session()
    role = 'your-sagemaker-role'  # Update this with your SageMaker role ARN

    # Specify your S3 bucket and prefixes
    bucket = 'your-s3-bucket-name'
    output_prefix = 'sagemaker/model'  # Where the model artifacts will be stored
    train_prefix = 'data'  # The location of your training data

    train_data = f's3://{bucket}/{train_prefix}'

    # Create an estimator object for running a training job
    estimator = Estimator(
        image_uri='your-image-uri',  # Specify the training image
        role=role,
        train_instance_count=1,  # Number of instances for training
        train_instance_type='ml.m5.2xlarge',  # Type of instance for training
        output_path=f's3://{bucket}/{output_prefix}',
        sagemaker_session=sagemaker_session,
        hyperparameters={},
        metric_definitions=[
        {'Name': 'MAE', 'Regex': 'mae=([0-9\\.]+);'},
        {'Name': 'RMSE', 'Regex': 'rmse=([0-9\\.]+);'}
    ]
    )

    # Specify the path to the training script
    estimator.fit({'train': train_data})

if __name__ == "__main__":
    train_model_sagemaker()

# %%
# ins = [ml.m6i.xlarge, ml.trn1.32xlarge, ml.p2.xlarge, ml.m5.4xlarge, ml.m4.16xlarge, ml.m6i.12xlarge, ml.p5.48xlarge, ml.m6i.24xlarge, ml.p4d.24xlarge, ml.g5.2xlarge, ml.c5n.xlarge, ml.p3.16xlarge, ml.m5.large, ml.m6i.16xlarge, ml.p2.16xlarge, ml.g5.4xlarge, ml.c4.2xlarge, ml.c5.2xlarge, ml.c6i.32xlarge, ml.c4.4xlarge, ml.c6i.xlarge, ml.g5.8xlarge, ml.c5.4xlarge, ml.c6i.12xlarge, ml.c5n.18xlarge, ml.g4dn.xlarge, ml.c6i.24xlarge, ml.g4dn.12xlarge, ml.c4.8xlarge, ml.g4dn.2xlarge, ml.c6i.2xlarge, ml.c6i.16xlarge, ml.c5.9xlarge, ml.g4dn.4xlarge, ml.c6i.4xlarge, ml.c5.xlarge, ml.g4dn.16xlarge, ml.c4.xlarge, ml.trn1n.32xlarge, ml.g4dn.8xlarge, ml.c6i.8xlarge, ml.g5.xlarge, ml.c5n.2xlarge, ml.g5.12xlarge, ml.g5.24xlarge, ml.c5n.4xlarge, ml.trn1.2xlarge, ml.c5.18xlarge, ml.p3dn.24xlarge, ml.m6i.2xlarge, ml.g5.48xlarge, ml.g5.16xlarge, ml.p3.2xlarge, ml.m6i.4xlarge, ml.m5.xlarge, ml.m4.10xlarge, ml.c5n.9xlarge, ml.m5.12xlarge, ml.m4.xlarge, ml.m5.24xlarge, ml.m4.2xlarge, ml.m6i.8xlarge, ml.m6i.large, ml.p2.8xlarge, ml.m5.2xlarge, ml.m6i.32xlarge, ml.p4de.24xlarge, ml.p3.8xlarge, ml.m4.4xlarge]
