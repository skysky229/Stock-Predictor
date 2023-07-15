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



        
