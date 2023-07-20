# STOCK PREDICTION (WITH LSTM and KAFKA + SPARK for streaming)
Stock Predictor, with Kafka and Spark
The main objective of the project is for learning how the LSTM works and how to apply Kafka and Spark in data streaming.
In order to run the project: 
1. Initialize Kafka server and Zookeeper
2. Create a Kafka topic named "stock_pred" (any name should work, but in case other names are used, please change the config in config.py)
3. Run producer.py for starting to send messages 
4. Run spark_predict.py for receiving messages via Spark and print the prediction to the screen. The prediction will be saved in predictions.txt.

NOTES:
1. Kafka, Kafka-Python and Spark must be installed in advance.
2. The size of the input of the model is (60,1), which means that it needs 60 records to predict a following day. Therefore, any dataset with less than 60 records will not work properly. 
