# Stock Prediction (LSTM) and KAFKA + SPARK for streaming
Stock Predictor, with Kafka and Spark
The project includes training an LSTM model with the data of VNI index, which can be found at https://www.investing.com/indices/vn. The main objective of the project is for learning how the LSTM works and how to apply Kafka and Spark in data streaming.

## Prerequisite
1. Downloaded and extract Kafka from https://kafka.apache.org/downloads
2. Install required libraries:
```
pip install requirements.txt
```

## Make Predictions
1. Change the directory to Kafka folder, then start the zookeeper and the kafka server. The detailed guide is provided on the official website of Apache Kafka: https://kafka.apache.org/quickstart
2. Create a topic named kafka_pred:
```
./bin/kafka-topics.sh --create --topic kafka_pred --bootstrap-server localhost:9092
```
In case you want to create a topic with a different name, please change the topic inside the config.py file. 
3. Run spark_predict.py for receiving messages via Spark and make predictions Run producer.py for starting to send messages 
```
python3 spark_predict.py
```
4. Run producer.py for starting to send messages. The prediction will be saved in predictions.txt.
```
python3 producer.py
```
NOTES:
1. In case you want to make prediction with a different dataset, place it in the data folder and rename it to markets_historical_vnindex_ind_test.csv
2. The size of the input of the model is (60,1), which means that it needs 60 records to predict a following day. Therefore, any dataset with less than 60 records will not work properly. 
