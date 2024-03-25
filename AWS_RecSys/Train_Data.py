#%%
from sqlalchemy import create_engine
import pandas as pd
engine = create_engine('mysql+mysqlconnector://admin:Xie19970826!@database.cffidgh00pge.us-east-1.rds.amazonaws.com/mydb')
print(engine)
sql = """
SELECT userid, movieid, rating, row_number() over() r
FROM Rating
ORDER BY r desc
limit 100000
"""
df = pd.read_sql_query(sql, engine)[['userid', 'movieid', 'rating']]
df.to_csv('train_data.csv', index=False)

import boto3
s3 = boto3.client('s3',
    aws_access_key_id='xxx',
    aws_secret_access_key='xxx')
s3.upload_file('train_data.csv', '11695movie', 'data/train_data.csv')


# %%
