import pandas as pd
import json
from config import config
from kafka import KafkaProducer
import logging
import sys


def main():
     producer = KafkaProducer(bootstrap_servers=config['server'])
     topicName = config['topic']

     df = pd.read_csv("data/markets_historical_vnindex_ind_test.csv")

     for col in df.columns[1:4]:
          df[col] = pd.to_numeric(df[col].apply(lambda x: x.replace(",", "")))

     df['json'] = df.apply(lambda x: x.to_json(), axis=1)

     for id, row in df.iterrows():
          #print(row['json'])
          msg = json.dumps(row['json']).encode('utf-8')
          producer.send(topicName, msg)
          print("Sent success ", id)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())