import pandas as pd
import json
from config import config
from kafka import KafkaProducer
import logging
import sys
import time

def main():
     producer = KafkaProducer(bootstrap_servers=config['server'])
     topicName = config['topic']

     df = pd.read_csv("data/markets_historical_vnindex_ind_test.csv").sort_values(by='Date')
     df = df[['Date', 'Price']]

     df['Price'] = pd.to_numeric(df['Price'].apply(lambda x: x.replace(",", "")))
     
     for i in range(60,len(df)):
          chunk_df = df.iloc[i-60:i,]
          msg = json.dumps(chunk_df.to_dict()).encode('utf-8')
          producer.send(topicName, msg)
          print(msg)
          print("Sent success ", id)
          time.sleep(0.5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())

    