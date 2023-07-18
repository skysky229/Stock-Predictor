import pandas as pd
import json
from config import config
from kafka import KafkaProducer
import logging
import sys
import time

"""df = pd.read_csv("data/markets_historical_vnindex_ind_test.csv")
list_price = ["Price", "Open", "High", "Low"]
for i in list_price:
    df[i] = pd.to_numeric(df[i].apply(lambda x: x.replace(",", "")))

BOOTSTRAP_SERVER = 'localhost'
TOPIC = 'stock_pred'

producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])

col = []
for i in range(0, 7):
    col.append(df.columns[i])

def send(tmp):
    '''s = ""
    for i in range(0,6):
        s = s + col[i] + ": " + str(tmp[i]) + "-"'''
    s = {}
    for i in range(0, 7):
        s[col[i]] = tmp[i]
    print("Message: ", s)
    producer.send(TOPIC, value = json.dumps(s).encode('utf-8'))

import time
timing = 0.5
for i in range(df.shape[0]-1, -1, -1):
    tmp = list(df.loc[i])
    send(tmp)
    time.sleep(timing)"""

def main():
     producer = KafkaProducer(bootstrap_servers=config['server'])
     topicName = config['topic']

     df = pd.read_csv("data/markets_historical_vnindex_ind_test.csv").sort_values(by='Date')
     df = df[['Date', 'Price']]

     df['Price'] = pd.to_numeric(df['Price'].apply(lambda x: x.replace(",", "")))
     
     for id, row in df.iterrows():
          #print(row['json'])
          msg = json.dumps(row.to_dict()).encode('utf-8')
          producer.send(topicName, msg)
          print(msg)
          print("Sent success ", id)
          time.sleep(0.5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())

    