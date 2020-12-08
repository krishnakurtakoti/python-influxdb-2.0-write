#!/usr/bin/python
import requests
import uuid
import random
import time
import sys
import csv
import json

INFLUX_TOKEN='qCAYOyvOErIP_KaJssk_neFar-o7PdvHL64eWYCD_ofywR_J3iubktdB58A3TE-6sM7C61Gt8qOUPvc4t0WVBg=='
ORG="asz"
INFLUX_CLOUD_URL='localhost'
BUCKET_NAME='b'

# Be sure to set precision to ms, not s
QUERY_URI='http://{}:8086/api/v2/write?org={}&bucket={}&precision=ms'.format(INFLUX_CLOUD_URL,ORG,BUCKET_NAME)

headers = {}
headers['Authorization'] = 'Token {}'.format(INFLUX_TOKEN)

measurement_name = 'data_0_20200901'
# Increase the points, 2, 10 etc.
number_of_points = 1000
batch_size = 1000

data_end_time = int(time.time() * 1) #milliseconds

id_tags = []
for i in range(100):
    id_tags.append(str(uuid.uuid4()))

data = []

current_point_time = data_end_time

with open('/home/k/Downloads/influxData/data_0_20200901.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    print('Processed')
    _data_end_time = int(time.time() * 1000) - (100 * 1 * 1000)

    for row in csv_reader:
        _row = 0
        current_point_time = current_point_time - 1000
        _data_end_time =  _data_end_time + (1 * 1000)
        if row[0] == "TIMESTAMP":
            pass
        else:
            _add = int(time.time()) - int(row[0])
            #_row = int((int(row[0]) + 5847435 + 952068) * 1000)
            _row = int((int(row[0])) * 1000)
            print(_add)
        
        print(_data_end_time, row[0],_row, '\n')
        data.append("{measurement},location={location} POWER_A={POWER_A},POWER_B={POWER_B},POWER_C={POWER_C} {timestamp}"
            .format(measurement=measurement_name, location="reservoir", POWER_A=row[2], POWER_B=row[3], POWER_C=row[4], timestamp=_row))#timestamp=row[0]))......(data_end_time + 1000)


count = 0
if __name__ == '__main__':
  # Check to see if number of points factors into batch size
  count = 0
  if (  number_of_points % batch_size != 0 ):
    raise SystemExit( 'Number of points must be divisible by batch size' )
  # Newline delimit the data
  for batch in range(0, len(data), batch_size):
    time.sleep(10)  
    current_batch = '\n'.join( data[batch:batch + batch_size] )
    print(current_batch)
    r = requests.post(QUERY_URI, data=current_batch, headers=headers)
    count = count + 1
    print(r.status_code, count, data[count])