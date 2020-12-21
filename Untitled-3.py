#!/usr/bin/python
import os
import requests
import uuid
import random
import time
import sys
import csv
import json

#INFLUX_TOKEN='qCAYOyvOErIP_KaJssk_neFar-o7PdvHL64eWYCD_ofywR_J3iubktdB58A3TE-6sM7C61Gt8qOUPvc4t0WVBg=='
INFLUX_TOKEN='Gg6a5FSBu3pUquU-lreVvIkLGlCAI3xGeeMLNAqI72ogt0ABSZdub2Mk5U02q2FuOZrh5pyrMVbki6d1ciTszQ=='
ORG="asz"
#INFLUX_CLOUD_URL='localhost'
INFLUX_CLOUD_URL='192.168.0.106'
BUCKET_NAME='b'

# Be sure to set precision to ms, not s
QUERY_URI='http://{}:8086/api/v2/write?org={}&bucket={}&precision=ms'.format(INFLUX_CLOUD_URL,ORG,BUCKET_NAME)

headers = {}
headers['Authorization'] = 'Token {}'.format(INFLUX_TOKEN)

measurement_name = 'sample2'
# Increase the points, 2, 10 etc.
number_of_points = 1000
batch_size = 1000

data_end_time = int(time.time() * 1) #milliseconds

id_tags = []
for i in range(100):
    id_tags.append(str(uuid.uuid4()))

data = []

current_point_time = data_end_time
directory = "/home/k/Downloads/a"
os.getcwd()
print(os.getcwd())
os.chdir('/home/k/Downloads/a')
print(os.getcwd())
#os.chdir(r'/home/k/Downloads/_d')

data = []
for root,dirs,files in os.walk(directory):
    for file in files:
        #print(file)
        '''
        f = open('/home/rv/ncbi-blast-2.2.23+/db/output.blast')
        z = csv.reader(f, delimiter='\t')
        ...
        f.close()
        '''
        print(files)
        if file.endswith(".csv"):
            f=open(file, 'r')
            csv_reader = csv.reader(f, delimiter=',')
            #with open(file) as csv_file:
                #csv_reader = csv.reader(csv_file, delimiter=',')
            print('Processed')
            for row in csv_reader:
                _row = 0
                 #current_point_time = current_point_time - 1000
                #_data_end_time =  _data_end_time + (1 * 1000)
                if row[0] == "TIMESTAMP":
                    pass
                else:
                    #_add = int(time.time()) - int(row[0])
                    _row = int((int(row[0])) * 1000 * 1000 * 1000)
                     #_row = int((int(row[0])) * 1000)
                #print(_add)
        
                #print(#_data_end_time, 
                #    row[0],_row, row[1], row[2], row[3], row[4],'\n')
                data.append("{measurement},DS_ID={location} POWER_A={POWER_A},POWER_B={POWER_B},POWER_C={POWER_C},PF_A={PF_A},PF_B={PF_B},PF_C={PF_C},PUMP_A={PUMP_A},PUMP_B={PUMP_B},PUMP_C={PUMP_C},LIGHT_A={LIGHT_A},LIGHT_B={LIGHT_B},LIGHT_C={LIGHT_C},LIFT_A={LIFT_A},LIFT_B={LIFT_B},LIFT_C={LIFT_C},LIFT_TOTAL={LIFT_TOTAL},LIGHT_TOTAL={LIGHT_TOTAL},PUMP_TOTAL={PUMP_TOTAL} {timestamp}"
                    .format(measurement="hdb2", location=row[1], POWER_A=row[2], POWER_B=row[3], POWER_C=row[4], PF_A=row[5], PF_B=row[6], PF_C=row[7], PUMP_A=row[8], PUMP_B=row[9], PUMP_C=row[10], LIGHT_A= row[11], LIGHT_B=row[12], LIGHT_C=row[13], LIFT_A=row[14], LIFT_B=row[15], LIFT_C=row[16], LIFT_TOTAL=row[17], LIGHT_TOTAL=row[18], PUMP_TOTAL=row[19], timestamp=_row)) 
            print(data)
            print(file)

            #print(f)
            #  perform calculation
            f.close()

        '''   
        '''
        time.sleep(2)    
        print(files)
    '''
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
    '''
'''
import requests
#import urllib3
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
#export PYTHONWARNINGS="ignore:Unverified HTTPS request"

WRITE_URI='https://ts-gs53ym52ntpu7vizl.influxdata.rds.aliyuncs.com:3242/write'

params = (
('db', 'tt'),
('u', 'evvo'),
('p', 'p@ssword123'),
)

data = 'hdb,block_id=0 p1=0.64,p2=0.34 1434055562000000000'

response = requests.post(WRITE_URI, verify=False, params=params, data=data)
print(response.status_code)
'''
count = 0
WRITE_URI='https://ts-gs53ym52ntpu7vizl.influxdata.rds.aliyuncs.com:3242/write'

params = (
('db', 'tt'),
('u', 'evvo'),
('p', 'p@ssword123'),
)
'''

WRITE_URI='http://localhost:8086/write'

params = (
('db', 'devconnected-three'),
('u', 'username'),
('p', 'strongpassword'),
)
'''
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
    #r = requests.post(QUERY_URI, data=current_batch, headers=headers)
    r = requests.post(WRITE_URI, verify=False, params=params, data=current_batch)
    count = count + 1
    print(r.status_code, count, data[count])
