#%%
from confluent_kafka import Consumer, KafkaError, TopicPartition
from prometheus_client import start_http_server, Gauge, Counter
# from flaskr.trainingdata.fetch_from_API import *
from datetime import datetime, timedelta
import re
import mlflow
import gc
import csv

test_recommend_num = 2000
max_log_num = 50000

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mlip-team25',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Enable auto commit
    'auto.commit.interval.ms': 1000,
}

now = datetime.now() - timedelta(days=1)
start_of_today_ = datetime(now.year, now.month, now.day)
start_of_today = start_of_today_.timestamp()

consumer = Consumer(**conf)
topic = 'movielog25'


# Find the starting offset for today
topic_partition = TopicPartition(topic, 0)  # Assuming single partition, partition ID 0
topic_partition.offset = int(start_of_today * 1000)  # Kafka expects milliseconds
offsets = consumer.offsets_for_times([topic_partition])

#Promethes for monitoring
prometheus_port = 8000  # Port for the metrics server

try:
    start_http_server(prometheus_port)
    print(f"Metrics server started on port {prometheus_port}")
except OSError as e:
    print(f"Could not start metrics server on port {prometheus_port}. It may already be running. Error: {e}")
CLICK_RATE_even = Gauge('click_rate_even', 'The rate of clicks of even model')
CLICK_RATE_odd = Gauge('click_rate_odd', 'The rate of clicks of odd model')
Mean_Reciprocal_Rank_even = Gauge('mrr_even', 'The mean reciprocal rank of even model')
Mean_Reciprocal_Rank_odd = Gauge('mrr_odd', 'The mean reciprocal rank of odd model')
http_responses_200 = Counter('http_responses_200', 'Count of HTTP 200 responses')
http_responses_500 = Counter('http_responses_500', 'Count of HTTP 500 responses')
http_responses_0 = Counter('http_responses_0', 'Count of HTTP 0 responses')

with open("log.csv", mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=["time", "click_rate_odd", "click_rate_even",
                                              "mrr_odd", "mrr_even"])
    writer.writeheader()

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

    CTR_total = 0
    MRR_total = 0
    max_round = 10

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

            if "recommendation request 17645-team25.isri.cmu.edu:8082, status 200" in message:
                http_responses_200.inc()
            elif "recommendation request 17645-team25.isri.cmu.edu:8082, status 500" in message:
                http_responses_500.inc()
            elif "recommendation request 17645-team25.isri.cmu.edu:8082, status 0" in message:
                http_responses_0.inc()
            else:
                pass

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
                if ('+' not in movie_id):
                    # a simple version of movie id check to accelerate the testing process
                    continue
                if (minutes.isdigit() == False):
                    continue

                if user_id in click_count_dict:
                    if movie_id in recommend_dict[user_id]:
                        click_count_dict[user_id] = 1
                        recom = recommend_dict[user_id].replace(" ","").split(',')
                        try: 
                            rank = recom.index(movie_id)+1
                        except:
                            rank = 1
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
                zero_count_odd = sum(1 for id, value in click_count_dict.items() if value == 0 and int(id)%2==1)
                zero_count_even = sum(1 for id, value in click_count_dict.items() if value == 0 and int(id)%2==0)
                one_count_odd = sum(1 for id, value in click_count_dict.items() if value == 1 and int(id)%2==1)
                one_count_even = sum(1 for id, value in click_count_dict.items() if value == 1 and int(id)%2==1)
                
                click_rate_acc_odd = one_count_odd / (zero_count_odd + one_count_odd + 0.000001)
                click_rate_acc_even = one_count_even / (zero_count_even + one_count_even + 0.000001)

                zero_count_MRR_odd = sum(1 for id, value in MRR_dict.items() if value == 0 and int(id)%2==1)
                zero_count_MRR_even = sum(1 for id, value in MRR_dict.items() if value == 0 and int(id)%2==0)

                nonzero_count_MRR_odd = sum(1 for id, value in MRR_dict.items() if value > 0 and int(id)%2==1)
                nonzero_count_MRR_even = sum(1 for id, value in MRR_dict.items() if value > 0 and int(id)%2==0)
                
                non_zero_sum_odd = sum(value for id, value in MRR_dict.items() if value > 0 and int(id)%2==1)
                non_zero_sum_even = sum(value for id, value in MRR_dict.items() if value > 0 and int(id)%2==0)

                mrr_odd = non_zero_sum_odd / (zero_count_MRR_odd + nonzero_count_MRR_odd + 0.000001)
                mrr_even = non_zero_sum_even / (zero_count_MRR_even + nonzero_count_MRR_even + 0.000001)

                print(f"\nEvaluation round {count_eva_round}: ")
                print('Time: ', last_log_time, '\tClick Rate Odd: ', click_rate_acc_odd, '\tClick Rate Even: ', click_rate_acc_even, 
                       '\tMean Reciprocal Rank Odd: ', mrr_odd, '\tMean Reciprocal Rank Even: ', mrr_even)
                # begin a new round of eva
                click_count_dict = {}
                recommend_dict = {}
                MRR_dict = {}
                count_log = 0
                count_recom = 0
                count_eva_round += 1

                #Prometheus update metrics
                CLICK_RATE_odd.set(click_rate_acc_odd)
                CLICK_RATE_even.set(click_rate_acc_even)
                Mean_Reciprocal_Rank_odd.set(mrr_odd)
                Mean_Reciprocal_Rank_even.set(mrr_even)
                gc.collect()
                with open("log.csv", mode='a', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=["time", "click_rate_odd", "click_rate_even",
                                              "mrr_odd", "mrr_even"])
                    row = {"time": datetime.now(), "click_rate_odd": click_rate_acc_odd, "click_rate_even": click_rate_acc_even,
                            "mrr_odd": mrr_odd, "mrr_even": mrr_even}
                    writer.writerow(row)


    finally:
        consumer.close()
