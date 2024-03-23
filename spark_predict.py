from tokenize import String
from xml.dom.minicompat import StringTypes
import pandas as pd
import numpy as np
import json
from pyspark.sql import SparkSession
from config import config
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.pandas as ps
import os
from tensorflow.keras.models import load_model
import joblib

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'

out_df = pd.DataFrame(columns=['Date', 'Price', 'Prediction'])

model = load_model("model/stock_prediction.h5")
scaler = joblib.load('model/scaler.sc') 

spark = SparkSession \
            .builder \
            .master("local[*]")\
            .appName("APP") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") # Only interested in ERROR-level logs; the others (such as WARN) will be ignored.

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['server']) \
    .option("subscribe", config['topic']) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 1) \
    .load()

df.printSchema()

json_schema = ArrayType(StructType([
    StructField("Date", StringType(), True),
    StructField("Price", DoubleType(), True)
]))

parsed_df = df.withColumn("json_conv", from_json(df.value.cast("string"), json_schema)).select(explode("json_conv").alias("Data")).select("data.*")

def prediction(msg, id):
    chunk_df = msg.toPandas()

    if (chunk_df.shape[0] == 0):
        return 
    
    chunk_df = chunk_df.set_index("Date")
    
    X_test = scaler.fit_transform(chunk_df.values)
    X_test = np.array(X_test)
    X_test = np.reshape(X_test, (1, -1, 1))
    pred = model.predict(X_test)
    pred = scaler.inverse_transform(pred)
    print(pred)
    # Write to the txt file
    with open("predictions.txt", "a") as f:
        f.write(str(pred[0][0]) + "\n")     
    
out = parsed_df \
        .writeStream \
        .format("console") \
        .foreachBatch(prediction) \
        .trigger(processingTime='0.5 seconds') \
        .start()  \
        .awaitTermination()
