from tokenize import String
from xml.dom.minicompat import StringTypes
import pandas as pd
import json
from pyspark.sql import SparkSession
from config import config
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sklearn import preprocessing

spark = SparkSession \
            .builder \
            .appName("APP") \
            .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['server']) \
    .option("subscribe", config['topic']) \
    .option("startingOffsets", "latest") \
    .load()

df.printSchema()

schema = ArrayType(StructType([
    StructField("Date", StringType()), \
    StructField("Price", DoubleType()), \
]))



new_df = df.select(\
                from_json(df.value.cast("string"),\
                StructType().add("Date", StringType())\
                            .add("Price", DoubleType())).alias("INFO")\
                ,"key", "timestamp"
                ).select("INFO.*")

new_df.printSchema()


out = new_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()




        
