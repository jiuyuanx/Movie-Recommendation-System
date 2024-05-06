'''
We fetch 2000 rating log from 2 days ago and use that to train SVD model. 
'''

from confluent_kafka import Consumer, KafkaError, TopicPartition
import time
import csv
from flaskr.trainingdata.fetch_from_API import *

def fetch_train_data():
    log_num = 2000

    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mlip-team25',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Enable auto commit
        'auto.commit.interval.ms': 1000,
    }

    consumer = Consumer(**conf)
    topic = 'movielog25'

    # Calculate start and end timestamps for the previous day
    current_time = time.time()
    one_day_seconds = 24 * 60 * 60
    two_day_seconds = 2 * one_day_seconds

    start_of_2_day_ago = current_time - two_day_seconds
    end_of_2_day_ago = start_of_2_day_ago + one_day_seconds * 2

    # Find the starting offset for the previous day
    topic_partition = TopicPartition(topic, 0)  # Assuming single partition, partition ID 0
    topic_partition.offset = int(start_of_2_day_ago * 1000)  # Kafka expects milliseconds
    offsets = consumer.offsets_for_times([topic_partition])

    if offsets[0].offset == -1:
        print("No messages found for the specified time range.")
    else:
        # Seek to the start of the previous day
        consumer.assign([offsets[0]])
        print('Reading Kafka Broker and filtering logs for the previous day')

        count = 0
        # Open a CSV file to write the validated data
        with open('../data/training_set/rate_log.csv', mode='w', newline='') as rate_file, \
            open('../data/training_set/user.csv', mode='w', newline='') as user_file:
        
            rate_writer = csv.writer(rate_file)
            # Write CSV headers
            rate_writer.writerow(["user_id","movie_id","rate"])

            user_writer = csv.writer(user_file)
            # Write CSV headers
            user_writer.writerow(["user_id","age","occupation","gender"])
            try:
                while True:
                    if count == log_num:
                        break

                    msg = consumer.poll(1.0)

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

                    message_timestamp = msg.timestamp()[1]  # Get message timestamp
                    # Break the loop if the message is beyond the end of the previous day
                    if message_timestamp > end_of_2_day_ago * 1000:
                        break

                    message = msg.value().decode('utf-8')
                    
                    if "GET /rate" in message:

                        # rate is digit between 0-5, and is digit
                        rate = message[-1]
                        if (rate.isdigit() == False or int(rate) < 0 or int(rate) > 5):
                            continue

                        # user_id is int after " " is replaced
                        parts1 = message.split(',')
                        user_id = parts1[1].replace(" ","")
                        if (user_id.isdigit() == False):
                            continue

                        user_info = get_user_info(str(user_id))
                        if (user_info != None):
                            age = user_info['age']
                            occupation = user_info.get('occupation', 'N/A')
                            gender = user_info['gender']

                            # Scheme Check
                            # 1. age out of range
                            if not (0 < age <= 120):
                                continue
                            # 2. gender not F or M
                            if gender != 'F' and gender != 'M':
                                continue
                            user_writer.writerow([user_id,age,occupation,gender])
                        else:
                            continue


                        # movie_id can be found through api
                        parts2 = message.split('/')
                        movie_id = parts2[2][:-2].replace(" ","")
                        if (get_movie_info(str(movie_id)) == None):
                            continue
                        
                        rate_writer.writerow([user_id, movie_id, rate])
                        print("Progress: ", count+1, '/', log_num)
                        count += 1

            finally:
                consumer.close()