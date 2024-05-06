import csv
from flaskr.trainingdata.fetch_from_API import *
from tqdm import tqdm

usr_num = 800000

with open('../data/cache/user.csv', mode='w', newline='') as user_file:
    user_writer = csv.writer(user_file)
    user_writer.writerow(["user_id","age","occupation","gender"])

    for i in tqdm(range(usr_num)):
        user_info = get_user_info(i)
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
            user_writer.writerow([i,age,occupation,gender])


