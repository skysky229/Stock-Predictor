from tokenize import String
from xml.dom.minicompat import StringTypes
import pandas as pd
import numpy as np
import json
from pyspark.sql import SparkSession
from config import config
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sklearn import preprocessing
import os
from tensorflow.keras.models import load_model
import joblib

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'

out_df = pd.DataFrame(columns=['Date', 'Price', 'Prediction'])

model = load_model("model/stock_prediction.h5")

spark = SparkSession \
            .builder \
            .appName("APP") \
            .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") # Only interested in ERROR-level logs; the others (such as WARN) will be ignored.

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['server']) \
    .option("subscribe", config['topic']) \
    .option("startingOffsets", "latest") \
    .load()

df.printSchema()

inpSchema = StructType([
    StructField("Date", StringType(), True), \
    StructField("Price", DoubleType(), True), \
])

new_df = df.select(\
                from_json(df.value.cast("string"),\
                inpSchema, {"mode" : "PERMISSIVE"}).alias("INFO")
                ).select("INFO.*")

new_df.printSchema()

def prediction(row):
    global out_df
    if (out_df.shape[0] < 59):
        out_df.loc[out_df.shape[0]] = [row['Date'], row['Price'], 0]
    else :
        pred_inp = out_df['Price'].tail(59).append(row['Price'])
        pred_inp = np.reshape(pred_inp, (pred_inp.shape[0], 1))
        pred_val = model.predict(pred_inp)
        out_df.loc[out_df.shape[0]] = [row['Date'], row['Price'], pred_val]
     
    print(out_df, out_df.shape)
out = new_df \
        .writeStream \
        .foreach(prediction) \
        .trigger(processingTime='5 seconds') \
        .start()  \
        .awaitTermination()
