#%%
import time
import numpy as np
# from kafka import KafkaConsumer
import mysql.connector
from tqdm.auto import tqdm
import pandas as pd
import json  
import re
from fetch_api import  get_movie_info,  get_user_info
from sqlalchemy import create_engine
import argparse
from datetime import datetime, timedelta
from confluent_kafka import Consumer, TopicPartition, KafkaException, KafkaError, OFFSET_BEGINNING, OFFSET_END
import uuid

# Kafka consumer configuration
topic_name = "movielog25"  # Replace with your actual team number
bootstrap_servers = ['localhost:9092']

# Initialize Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': f'mlip-team25-{uuid.uuid4()}',
    'auto.offset.reset': 'earliest',  # Can be 'earliest', 'latest', 'none'
    'enable.auto.commit': False,
    'auto.commit.interval.ms': 1000,
}
consumer = Consumer(**conf)
# consumer.subscribe([topic_name])

#%%
# ------------------------------------RDS MYSQL Connection------------------------------------
conn = mysql.connector.connect(
    user='admin', password='your-password', 
    host='your-hostname', 
    database='mydb'
)
cur = conn.cursor()
engine = create_engine('mysql+mysqlconnector://your-username:your-password@your-hostname:3306/mydb')
print(engine)
#%%
def parse_consumer(text):
    #2023-12-27T17:23:10,99788,GET /data/m/the+brothers+2001/0.mpg -------streaming history
    #2023-12-27T19:10:59,160766,GET /rate/the+tigger+movie+2000=4 --------rating score

    components = text.split(',')
    timestamp = components[0]
    user_id = components[1]
    
    movie_part=np.nan
    movie_score=np.nan
    try:
        if '=' not in text: #streaming history
            #GET /data/m/the+brothers+2001/0.mpg"
            
            movie_path = components[2].split('/')
            movie_name_id = movie_path[3]
            movie_part = int(movie_path[4].split('.')[0])
        else: #rating score
            #GET /rate/the+tigger+movie+2000=4
            movie_path = components[2].split('/')
            detail = movie_path[2].split('=')
            movie_name_id = detail[0]
            movie_score = int(detail[1])
    except Exception as e:
        return 'None','None',0,0

    
    return user_id, movie_name_id, movie_part, movie_score, timestamp
    

#%%
# ------------------------------------CONSUME KAFKA STREAM---------------------
# Parse command-line arguments
parser = argparse.ArgumentParser(description='Consume Kafka messages starting from a specified number of days ago.')
parser.add_argument('days1', type=float, default=0.1, help='Number of days ago from which to start consuming messages (can be a fraction)')
parser.add_argument('days2', type=float, default=0.0, help='Number of days ago from which to end consuming messages (can be a fraction)')
parser.add_argument('fetchUser', type=bool, default=False, help='If fetch User Info using API')
parser.add_argument('fetchMovie', type=bool, default=False, help='If fetch Movie Info using API')
args = parser.parse_args()
fetchUser = args.fetchUser
fetchMovie = args.fetchMovie
# Function to get the timestamp for N days before now, based on the command-line argument
def get_timestamp_n_days_ago(day1, day2):
    # day1: start day
    # day2: end day
    one_day_seconds = 24 * 60 * 60
    seconds1 = day1 * one_day_seconds
    seconds2 = day2 * one_day_seconds
    timestamp1 = (datetime.now()- timedelta(seconds=seconds1)).timestamp()
    timestamp2 = (datetime.now()- timedelta(seconds=seconds2)).timestamp()

    print("Timestamp start:", datetime.fromtimestamp(timestamp1), "Timestamp end:",  datetime.fromtimestamp(timestamp2))
    return int(timestamp1* 1000), int(timestamp2*1000) # Kafka expects timestamps in milliseconds

# #Reset Consumer!!!
# partition = 0
# # offset = int(timestamp2*1000)
# tp = TopicPartition(topic_name, 0, 0)
# consumer.offsets_for_times([tp])

# Function to find and set the offset to start consuming from, for each partition
def set_consumer_offsets_to_n_days_ago(consumer, topic, day1, day2):
    n_days_ago_timestamp, current = get_timestamp_n_days_ago(day1, day2)

    # Get all partitions for the topic
    metadata = consumer.list_topics(topic)
    partitions = [0]#metadata.topics[topic].partitions.keys()

    # Convert timestamps to offsets
    start_offsets = {}
    end_offsets = {}
    for partition in partitions:
        topic_partitions = [TopicPartition(topic, partition, current) for partition in partitions]
        end_offset = consumer.offsets_for_times(topic_partitions)
        consumer.assign([end_offset[0]])
        end_offset = end_offset[0].offset
        end_offsets[partition] = end_offset

        topic_partitions = [TopicPartition(topic, partition, n_days_ago_timestamp) for partition in partitions]
        start_offset = consumer.offsets_for_times(topic_partitions)
        consumer.assign([start_offset[0]])
        start_offset = start_offset[0].offset
        start_offsets[partition] = start_offset
        print("Start Offset:", start_offset, "End Offset:", end_offset)

    # Reassign consumer to the starting offsets
    for partition, offset in start_offsets.items():
        tp = TopicPartition(topic, partition, offset)
        consumer.assign([tp])  # This should ideally be a list of all partitions you're working with
        # print("Offset:", offset)
        consumer.seek(TopicPartition(topic, partition, offset))
    total_messages = sum(end_offsets[p] - start_offsets[p] for p in partitions)
    print("Start Offsets: ", start_offsets, "End Offsets: ", end_offsets)
    print(f"Total Messages to Consume: {total_messages}")

    return total_messages
total_messages = 0
while total_messages<=0:
# Set up your topic and adjust consumer offsets based on the specified number of days
    total_messages= set_consumer_offsets_to_n_days_ago(consumer, topic_name, args.days1, args.days2)
    # total_messages = set_consumer_offsets_to_n_days_ago(consumer, topic_name, 0.0001, 0)
print("Total Messages:", total_messages)

#%%
error = 0
start_time=time.time()
watches = []
ratings = []
counter = 0
try:
    pbar = tqdm(total=total_messages, dynamic_ncols=True, leave=False, position=0, desc='Kafka Reading...', ncols=5) 
    while True:
        msg = consumer.poll(timeout=1.0)  # Adjust the timeout as needed
        counter+=1
        if counter>total_messages:
            break
        pbar.set_postfix(num_processed=counter)
        pbar.update(1)
        # print(msg.value().decode('utf-8'))
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Once we reach the end of the data for the previous day, we can break out of the loop
                print('End of partition reached or end of previous day {0}/{1}'.format(msg.topic(), msg.partition()))
                break
            else:
                print(msg.error())
            continue
        try:
            text = msg.value().decode("utf-8")
            # print("text", text)
            user_id, movie_name_id, movie_part, movie_score, timestamp = parse_consumer(text)
            user_id = int(user_id)

            # Format data to MySQL RDS format
            if np.isnan(movie_score) and movie_part != np.nan: #watch
                watches.append([user_id, movie_name_id, movie_part, timestamp])
            elif ~np.isnan(movie_score): #rate
                ratings.append([user_id, movie_name_id, movie_score, timestamp])
            else:
                pass
        except Exception as e:
            # print(error, e)
            error+=1
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    consumer.close()

end_time = time.time()
print("Fetch spending time:",end_time-start_time)

df_watch_raw = pd.DataFrame(watches, columns = ['userid', 'movieid', 'movieparts', 'watch_time'])
df_rating = pd.DataFrame(ratings, columns = ['userid', 'movieid', 'rating', 'rating_time'])
import gc
del watches, ratings
gc.collect()
# display(df_watch_raw)
# display(df_rating)

#%%
#for each user-movie, get the last watched movie parts
df_watch = df_watch_raw.groupby(['userid', 'movieid' ])[['movieparts', 'watch_time']].max().reset_index()
df_movie = list(set(df_watch['movieid']) | set(df_rating['movieid']))
# df_user, df_movie

#%%
def createDataFrame(list_dicts):
    if len(list_dicts)>0:
        default_keys = next(item for item in list_dicts if item is not None).keys()
        default_dict = {key: None for key in default_keys}
        res = [item if item is not None else default_dict for item in list_dicts]
        df = pd.DataFrame(res)
    else:
        df = pd.DataFrame(list_dicts)
    return df



#%%
#------------------------------------FETCH MOVIE API--------------------------
movie_sql = """
select movieid from Movie;
"""
cur.execute(movie_sql)
# Fetch and process all results to ensure no unread results are left
all_movies = cur.fetchall()
all_movies = [i[0] for i in all_movies if i[0] !='']

movie_api = []
movie_exists = []
movie_list = ['id', 'tmdb_id', 'imdb_id', 'title', 'original_title', 'adult', 'belongs_to_collection', 'budget', 'genres', 'homepage', 'original_language', 'overview', 'popularity', 'poster_path', 'production_companies', 'production_countries', 'release_date', 'revenue', 'runtime', 'spoken_languages', 'status', 'vote_average', 'vote_count']
for m in tqdm(df_movie, "Fetching Movie Information..."):
    if m not in all_movies:
        if fetchMovie: #fetch from API
            movie_info = get_movie_info(m)
        else:
            movie_info = {i: None for i in movie_list}
            movie_info['id'] = m

        if movie_info != None:
            movie_api.append(movie_info)
    else:
        movie_exists.append(m)

df_movie_info = createDataFrame(movie_api)

def movie_feature(dff): #turn list of dictionary to list of strings with only the name
    df = dff.copy()
    df['genres'] = dff['genres'].apply( lambda x: [i['name'] for i in x] if x is not None else None)
    df['genre1'] = df['genres'].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    df['genre2'] = df['genres'].apply(lambda x: x[1] if x is not None and len(x) > 1 else None)
    df['genre3'] = df['genres'].apply(lambda x: x[2] if x is not None and len(x) > 2 else None)

    df['belongs_to_collection'] = dff['belongs_to_collection'].apply(lambda x:x['name'] if x is not None and x != {} else None)
    df['production_companies'] = dff['production_companies'].apply(lambda x:x[0]['name'] if x is not None and len(x)>0 else None)
    df['production_countries'] = dff['production_countries'].apply(lambda x:x[0]['name'] if x is not None and len(x)>0 else None)
    df['spoken_languages'] = dff['spoken_languages'].apply(lambda x:x[0]['name'] if x is not None and len(x)>0 else None)
    df = df.drop(columns=['genres'])
    df = df.rename(columns = {'id':'movieid'})
    return df

if len(df_movie_info)>0:
    df_movie_info  = movie_feature(df_movie_info)

#%%

#%%
#------------------------------------Filter out Movie, User that are not in API-------------------------
if fetchMovie:
    if len(df_movie_info)>0:
        df_rating = df_rating[df_rating['movieid'].isin(df_movie_info['movieid']) | df_rating['movieid'].isin(movie_exists)]
        df_watch = df_watch[df_watch['movieid'].isin(df_movie_info['movieid'])| df_watch['movieid'].isin(movie_exists)]
    else:
        df_rating = df_rating[df_rating['movieid'].isin(movie_exists)]
        df_watch = df_watch[df_watch['movieid'].isin(movie_exists)]
else:
    pass

#%%
df_user =  list(set(df_watch['userid']) | set(df_rating['userid']))

#%%
#------------------------------------FETCH USER API--------------------------
user_sql = """
select userid from User;
"""
cur.execute(user_sql)
# Fetch and process all results to ensure no unread results are left
all_users = cur.fetchall()
all_users = [i[0] for i in all_users]

user_api = []
user_exists = []
for u in tqdm(df_user, "Fetching User Information..."):
    if u not in all_users:
        if fetchUser:
            user_info = get_user_info(u)
        else:
            user_info ={'user_id': u, 'age': None, 'occupation': None, 'gender': None}
        if user_info != None:
            user_api.append(user_info)
    else:
        user_exists.append(u)
        


df_user_info = createDataFrame(user_api)
# df_user_info

if fetchUser:
    if len(df_user_info)>0:
        df_rating = df_rating[df_rating['userid'].isin(df_user_info['user_id']) | df_rating['userid'].isin(user_exists) ]
        df_watch = df_watch[df_watch['userid'].isin(df_user_info['user_id']) | df_watch['userid'].isin(user_exists)]
    else:
        df_rating = df_rating[df_rating['userid'].isin(user_exists) ]
        df_watch = df_watch[df_watch['userid'].isin(user_exists)]
else:
    pass


#%%
# ------------------------------------UPDATE USER TABLE------------------------------------
from sqlalchemy import create_engine, text
if len(df_user_info)>0:
    df_user_info = df_user_info.rename(columns = {'user_id':'userid'}).drop_duplicates().applymap(lambda x: None if pd.isna(x) else x)
    columns = ', '.join(df_user_info.columns)  # Column names
    placeholders = ', '.join([f':{col}' for col in df_user_info.columns])  # Named placeholders

    sql_template = f"INSERT IGNORE INTO User ({columns}) VALUES ({placeholders});"

    # Execute the INSERT IGNORE for each row in the DataFrame
    with engine.connect() as con:
        trans = con.begin()
        try:
            for index, row in df_user_info.iterrows():
                try:
                    con.execute(text(sql_template), row.to_dict())
                except Exception as e:
                    print(e)
            trans.commit()
        except:
            trans.rollback()
            pass

print("User Table Update Finished...")
# df_u = pd.read_sql_table('User', con=engine)
# df_u



#%%
# ------------------------------------UPDATE MOVIE TABLE------------------------------------
if len(df_movie_info)>0:

    df_movie_info = df_movie_info.rename(columns = {'id':'movieid'}).drop_duplicates().applymap(lambda x: None if pd.isna(x) else x)
    columns = ', '.join(df_movie_info.columns)  # Column names
    placeholders = ', '.join([f':{col}' for col in df_movie_info.columns])  # Named placeholders
    sql_template = f"INSERT IGNORE INTO Movie ({columns}) VALUES ({placeholders});"
    with engine.connect() as con:
        trans = con.begin()
        try:
            for index, row in df_movie_info.iterrows():
                try:
                    con.execute(text(sql_template), row.to_dict())
                except Exception as e:
                    print(e)
            trans.commit()
        except:
            trans.rollback()
            pass
print("Movie Table Update Finished...")
# df_m = pd.read_sql_table('Movie', con=engine)
# df_m


#%%
#------------------------------------UPDATE RATING TABLE------------------------------------

for i in range(len(df_rating)):
    row = df_rating.iloc[i]
    sql_rating = f"""
    INSERT INTO Rating (userid, movieid, rating, rating_time) 
    VALUES ({row['userid']}, '{row['movieid']}', {row['rating']}, '{row['rating_time']}') 
    ON DUPLICATE KEY UPDATE 
    rating = VALUES(rating), 
    rating_time = VALUES(rating_time);
    """
    try:
        cur.execute(sql_rating)
    except Exception as e:
        print(e) 

conn.commit()
print("Rating Table Update Finished...")
# pd.read_sql_table('Rating', con=engine)
#%%
#------------------------------------UPDATE WATCH TABLE------------------------------------

for i in range(len(df_watch)):
    row = df_watch.iloc[i]
    sql_watch = f"""
    INSERT INTO Watch (userid, movieid, movieparts, watch_time) 
    VALUES ({row['userid']}, '{row['movieid']}', {row['movieparts']}, '{row['watch_time']}') 
    ON DUPLICATE KEY UPDATE 
    movieparts = VALUES(movieparts), 
    watch_time = VALUES(watch_time);
    """
    try:
        cur.execute(sql_watch)
    except Exception as e:
        print(e) 

conn.commit()
print("Watch Table Update Finished...")
# pd.read_sql_table('Watch', con=engine)

#%%
# 428297 in df_user, 428297 not in all_users, 495896 in [i['user_id'] for i in user_api], get_user_info(495896)


#%%
# while cur.nextset():
#     pass
#%%
# read = "SELECT * FROM Watch"
# cur.execute(read)

# # Fetch and process all results to ensure no unread results are left
# rows = cur.fetchall()
# for row in rows:
#     print(row)

#%%
# Cleanup
consumer.close()
cur.close()
conn.close()
