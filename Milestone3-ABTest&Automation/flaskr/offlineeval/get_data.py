from kafka import KafkaConsumer
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tqdm.auto as tqdm


topic_name = "movielog25"  # Replace N with your actual team number
bootstrap_servers = ['localhost:9092']

# Initialize Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id=None,  # Do not commit offsets
)

from kafka import TopicPartition
# Initialize Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id=None,  # Do not commit offsets
)
# Fetch all partitions for the topic
partitions = consumer.partitions_for_topic(topic_name)
if partitions is None:
    raise Exception(f"Topic {topic_name} not found.")

topic_partitions = [TopicPartition(topic_name, p) for p in partitions]

total_length = 0
# Calculate the total length of the stream
for tp in topic_partitions:
    consumer.seek_to_beginning(tp)
    earliest_offset = consumer.position(tp)
    consumer.seek_to_end(tp)
    latest_offset = consumer.position(tp)

    length = latest_offset - earliest_offset
    total_length += length

consumer.close()
print("total_length:", total_length)

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id=None,  # Do not commit offsets
)
max_messages = 72281118//10
messages = []
for message in consumer:
    if len(messages) > max_messages:
        break
    if len(messages)<10: #show sample 10 consumer message
        print(message)
    text = message.value.decode("utf-8")
    
    messages.append(text)
consumer.close()


def parse_consumer(text):
    components = text.split(',')
    timestamp = components[0]
    user_id = components[1]
    
    movie_score=np.nan
    movie_name_id=None 
    if 'GET /rate' in text: #rating score
        #GET /rate/the+tigger+movie+2000=4
        movie_path = components[2].split('/')
        detail = movie_path[2].split('=')
        movie_name_id = detail[0].replace('+', '_')
        movie_score = detail[1]

    
    return timestamp, user_id, movie_name_id, movie_score
    

import gc
gc.collect()

# Filter out only rating messages
messages = [msg for msg in messages if 'GET /rate' in msg]

data = map(parse_consumer, messages)
df_raw = pd.DataFrame(data,columns = ['timestamp', 'user_id', 'movie_name_id','movie_score'])
df_raw.to_csv('data/rate_log_ts.csv')


