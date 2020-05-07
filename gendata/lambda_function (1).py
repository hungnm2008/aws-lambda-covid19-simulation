import json
import csv
import random
from urllib import response
import datetime
import boto3

#import names
from pip._vendor.msgpack.fallback import xrange

# import requests


DataSet = []
ID_List = []


def random_date(start, l):
    current = start
    while l >= 0:
        curr = current + datetime.timedelta(minutes=random.randrange(120))  # during 2 hours
        yield curr
        l -= 1


def generate_random_data(lat, lon, num_rows):
    for _ in xrange(num_rows):
        hex1 = '%012x' % random.randrange(16 ** 12)  # 12 char random string
        flt = float(random.randint(0, 100))
        dec_lat = random.random() / 100
        dec_lon = random.random() / 100
        Longitude = lon + dec_lon
        Latitude = lat + dec_lat
        return Longitude, Latitude


def info_generate(num_rows):
    HealthStatus_List = [1] * 50 + [2] * 25 + [3] * 20 + [
        4] * 5  # Health Status: 1=Healthy (weight 50%) 2=Actively Infected (weight 25%)3=Recovered (Weight 20%)4=Dead(Weight 5%)
    startDate = datetime.datetime(2020, 5, 4, 12, 00, 00, 00)
    for i in xrange(num_rows):
        ID = str(34625) + str(random.randint(100000, 999999))
        if ID in ID_List:
            num_rows + 1
            continue
        else:
        #    Name = names.get_full_name()  # Name
            Gender = random.randint(0, 1)  # Gender: 1 Male, 0 Female
            Occupation = random.randint(1,
                                        50)  # Occupation: 1-50 tags of occupation. For example, 23 = Software Engineer
            Age = random.randint(16, 65)
            Income = random.randint(10000, 50000)
            District = random.randint(1, 10)
            HealthCare_Capacity = random.randint(1, 4)
            Location = generate_random_data(41.3851, 2.1734, 1)
            Health_Status = random.choice(HealthStatus_List)
            for x in random_date(startDate, 0):
                Timestamp = x.strftime("%d%m%y%H%M")  # number of timestamp

            DataSet.append(
                [ID, Gender, Age, Occupation, Income, District, HealthCare_Capacity, Health_Status, Location,
                 Timestamp])


# with open('/tmp/'+'test1.csv', 'w', newline='') as csvfile:
#     header = ["ID", "Gender", "Age", "Occupation", "Income", "District", "HealthCare_Capacity", "Health_Status",
#               "(Latitude,Longitude)",
#               "Timestamp"]
#     writer = csv.DictWriter(csvfile, fieldnames=header)
#     writer.writeheader()
#     info_generate(10)

#     for i in DataSet:
#         writer.writerow({'ID': i[0], 'Gender': i[1], 'Age': i[2], 'Occupation': i[3],
#                          'Income': i[4], 'District': i[5], 'HealthCare_Capacity': i[6],
#                          'Health_Status': i[7], '(Latitude,Longitude)': i[8], 'Timestamp': i[9]})
        # writer.writerow({'ID': i[0], 'Name': i[1], 'Gender': i[2], 'Age': i[3], 'Occupation': i[4],
        #                  'Income': i[5], 'District': i[6], 'HealthCare_Capacity': i[7],
        #                  'Health_Status': i[8], '(Latitude,Longitude)': i[9], 'Timestamp': i[10]})


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('simulatedataset')
    key='dataset.csv'
    with open('/tmp/dataset.csv', 'w', newline='') as csvfile:
        header = ["ID", "Gender", "Age", "Occupation", "Income", "District", "HealthCare_Capacity", "Health_Status",
                  "(Latitude,Longitude)",
                  "Timestamp"]
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        info_generate(10000)
        for i in DataSet:
            writer.writerow({'ID': i[0], 'Gender': i[1], 'Age': i[2], 'Occupation': i[3],
                             'Income': i[4], 'District': i[5], 'HealthCare_Capacity': i[6],
                             'Health_Status': i[7], '(Latitude,Longitude)': i[8], 'Timestamp': i[9]})

    bucket.upload_file('/tmp/dataset.csv',key)

