from confluent_kafka import Consumer, KafkaError, TopicPartition
from flaskr.trainingdata.fetch_from_API import *
from datetime import datetime
import re

test_recommend_num = 2000
max_log_num = 50000

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mlip-team25',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Enable auto commit
    'auto.commit.interval.ms': 1000,
}

now = datetime.now()
start_of_today_ = datetime(now.year, now.month, now.day)
start_of_today = start_of_today_.timestamp()

consumer = Consumer(**conf)
topic = 'movielog25'


# Find the starting offset for today
topic_partition = TopicPartition(topic, 0)  # Assuming single partition, partition ID 0
topic_partition.offset = int(start_of_today * 1000)  # Kafka expects milliseconds
offsets = consumer.offsets_for_times([topic_partition])

if offsets[0].offset == -1:
    print("No messages found for the specified time range.")
else:
    # Seek to the start of the previous day
    consumer.assign([offsets[0]])
    print('Online testing...')

    count_log = 0
    count_recom = 0
    count_eva_round = 0

    recommend_dict = {}
    click_count_dict = {}
    MRR_dict = {}

    try:
        while True:
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
            message = msg.value().decode('utf-8')
            

            if "GET /data/m" in message:
                if count_recom >= test_recommend_num:
                    count_log += 1

                parts1 = message.split(',')
                user_id = parts1[1].replace(" ","")

                parts2 = message.split('/')
                movie_id = parts2[-2].replace(" ","")
                minutes = parts2[-1].split('.')[0].replace(" ","")
                last_log_time = parts1[0]

                # Scheme Check
                if (user_id.isdigit() == False):
                    continue
                # if (get_movie_info(str(movie_id)) == None):
                #     continue
                if ('+' not in movie_id):
                    # a simple version of movie id check to accelerate the testing process
                    continue
                if (minutes.isdigit() == False):
                    continue

                if user_id in click_count_dict:
                    if movie_id in recommend_dict[user_id]:
                        click_count_dict[user_id] = 1
                        recom = recommend_dict[user_id].replace(" ","").split(',')
                        rank = recom.index(movie_id)+1
                        MRR_dict[user_id] = 1 / rank
                    else:
                        click_count_dict[user_id] = 0
                        MRR_dict[user_id] = 0

            if "recommendation request 17645-team25.isri.cmu.edu:8082, status 200" in message and count_recom < test_recommend_num:
                # process recommendation
                count_recom += 1
                match = re.search(r"result: (.*?), \d+ ms", message)
                if match:
                    movies_str = match.group(1)
                    userid = message.split(',')[1]
                    if userid not in click_count_dict:
                        click_count_dict[userid] = -1
                    if userid not in recommend_dict:
                        recommend_dict[userid] = movies_str
                    if userid not in MRR_dict:
                        MRR_dict[userid] = -1
                else:
                    print("No match found from recommendation log.")
                    continue

            if count_log == max_log_num - 1:
                zero_count = sum(1 for item in click_count_dict.values() if item == 0)
                one_count = sum(1 for item in click_count_dict.values() if item == 1)
                click_rate_acc = one_count / (zero_count + one_count + 0.000001)

                zero_count_MRR = sum(1 for item in MRR_dict.values() if item == 0)
                nonzero_count_MRR = sum(1 for item in MRR_dict.values() if item > 0)
                non_zero_sum = sum(item for item in MRR_dict.values() if item > 0)
                click_rate_acc = one_count / (zero_count + one_count + 0.000001)
                mrr = non_zero_sum / (zero_count_MRR + nonzero_count_MRR + 0.000001)

                print(f"\nEvaluation round {count_eva_round}: ")
                print('Time: ', last_log_time, '\tClick Rate: ', click_rate_acc, '\tMean Reciprocal Rank: ', mrr)
                # begin a new round of eva
                click_count_dict = {}
                recommend_dict = {}
                MRR_dict = {}
                count_log = 0
                count_recom = 0
                count_eva_round += 1

    finally:
        consumer.close()
        
