"""ACCEPTS DATA STREAMED ON PORT 6100"""

# run as $SPARK_HOME/bin/spark-submit client.py 

"""SENTIMENT ANALYSIS TRAINING MODULE"""
import numpy as np
import pickle
import sys
import json
import time
# from preprocess import preproc

from pyspark import SparkContext
from pyspark.streaming import StreamingContext, DStream
from pyspark.sql import SQLContext, Row, SparkSession

from collections import namedtuple


# config
sc = SparkContext("local[2]", "NetworkWordCount")
spark = SparkSession(sc)

# 1 second batch interval
ssc = StreamingContext(sc, 1)
sqc = SQLContext(sc)

# initiate streaming text from a TCP (socket) source:
lines = ssc.socketTextStream('localhost', 6100)

# just a tuple to assign names
fields = ("hashtag", "count")
Tweet = namedtuple('Tweet', fields)
# here we apply different operations on the tweets and save them to #a temporary sql table
(lines.flatMap(lambda text: text.split(" "))  # Splits to a list
 # Checks for    hashtag calls
 .filter(lambda word: word.lower().startswith("#"))
 .map(lambda word: (word.lower(), 1))  # Lower cases the word
 .reduceByKey(lambda a, b: a + b)
 # Stores in a Tweet Object
 .map(lambda rec: Tweet(rec[0], rec[1]))
 # Sorts Them in a dataframe
 .foreachRDD(lambda rdd: rdd.toDF().sort(desc("count"))
             # Registers only top 10 hashtags to a table.
             .limit(10).registerTempTable("tweets")))

# start streaming and wait couple of minutes to get enought tweets
ssc.start()

# import libraries to visualize the results
# get_ipython().run_line_magic('matplotlib', 'inline')
count = 0
while count < 5:

    time.sleep(5)
    top_10_tags = sqlContext.sql('Select hashtag, count from tweets')
    top_10_df = top_10_tags.toPandas()
    display.clear_output(wait=True)
    plt.figure(figsize=(10, 8))
    sns.barplot(x="count", y="hashtag", data=top_10_df)
    plt.show()
    count = count + 1
    print(count)

# stop streaming and wait couple of minutes to get enought tweets
# ssc.stop()
ssc.awaitTermination()