#%%
from sqlalchemy import create_engine
import pandas as pd
engine = create_engine('mysql+mysqlconnector://your-username')
print(engine)
sql = """
SELECT userid, movieid, rating, rating_time
FROM Rating
ORDER BY rating_time desc
limit 1000000
"""
df = pd.read_sql_query(sql, engine)[['userid', 'movieid', 'rating']]
df.to_csv('train_data.csv', index=False)

import boto3
s3 = boto3.client('s3',
    aws_access_key_id='your-access-key-id',
    aws_secret_access_key='your-secret-access-key')
s3.upload_file('train_data.csv', 'your-s3-bucket', 'data/train_data.csv')


# %%
