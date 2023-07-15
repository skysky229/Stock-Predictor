import json
from config import config
from kafka import KafkaConsumer
import logging
import sys

def main():
    consumer = KafkaConsumer(config['topic'], bootstrap_servers=config['server'])

    while(True):
        for message in consumer:
            print(json.loads(message.value.decode('utf-8')))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())


