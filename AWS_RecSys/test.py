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
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000,
}
consumer = Consumer(**conf)
# consumer.subscribe([topic_name])


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
def get_timestamp_n_days_ago(day1, day2):
    # day1: start day
    # day2: end day
    one_day_seconds = 24 * 60 * 60
    seconds1 = day1 * one_day_seconds
    seconds2 = day2 * one_day_seconds
    timestamp1 = (datetime.now()- timedelta(seconds=seconds1)).timestamp()
    timestamp2 = (datetime.now()- timedelta(seconds=seconds2)).timestamp()

    print("Timestamp start:", timestamp1, "Timestamp end:", timestamp2)
    return int(timestamp1* 1000), int(timestamp2*1000) # Kafka expects timestamps in milliseconds

def set_consumer_offsets_to_n_days_ago(consumer, topic, day1, day2):
    n_days_ago_timestamp, current = get_timestamp_n_days_ago(day1, day2)

    # Get all partitions for the topic
    metadata = consumer.list_topics(topic)
    partitions = [0, 1]#metadata.topics[topic].partitions.keys()

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
        print("Offset:", offset)
        consumer.seek(TopicPartition(topic, partition, offset))
    total_messages = sum(end_offsets[p] - start_offsets[p] for p in partitions)
    print("Start Offsets: ", start_offsets, "End Offsets: ", end_offsets)
    print(f"Total Messages to Consume: {total_messages}")

    return total_messages

# Set up your topic and adjust consumer offsets based on the specified number of days
total_messages = set_consumer_offsets_to_n_days_ago(consumer, topic_name, 1, 0)
# total_messages = set_consumer_offsets_to_n_days_ago(consumer, topic_name, 1, 0)
print("Total Messages:", total_messages)

#%%
error = 0
start_time=time.time()
watches = []
ratings = []
counter = 0
total_messages = 20
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
# %%
df_watch_raw
# %%
df_watch_raw.drop_duplicates()
# %%
