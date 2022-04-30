"""ACCEPTS DATA STREAMED ON PORT 6100"""

# run as $SPARK_HOME/bin/spark-submit client.py 

"""SENTIMENT ANALYSIS TRAINING MODULE"""
import numpy as np
import pickle
import sys
import json
# from preprocess import preproc

from pyspark import SparkContext
from pyspark.streaming import StreamingContext, DStream
from pyspark.sql import SQLContext, Row, SparkSession

# config
sc = SparkContext("local[2]", "NetworkWordCount")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)
sqc = SQLContext(sc)

# global vectorizer
# # vectorizer = HashingTF(inputCol='Tweet', outputCol='features')
# vectorizer.setNumFeatures(1000)

def getTweets(rdd):
	
	x = rdd.collect()
	if(len(x)>0):
		df = spark.createDataFrame(json.loads(x[0]).values() , schema = ["sentiment_value" , "tweet"]) #creates the dataframe
		df.show(truncate=False)
		print(json.loads(x[0]).values())

lines = ssc.socketTextStream('localhost', 6100)
lines = lines.flatMap(lambda line: json.loads(line))
lines = lines.filter(lambda line: line[0] != 'S')

lines.foreachRDD(getTweets)
ssc.start()
ssc.awaitTermination()