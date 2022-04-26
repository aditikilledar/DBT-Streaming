"""ACCEPTS DATA STREAMED ON PORT 6100"""

# run as $SPARK_HOME/bin/spark-submit client.py 

import numpy as np
import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext, DStream
from pyspark.sql import SQLContext, Row, SparkSession

# config
sc = SparkContext("local[2]", "NetworkWordCount")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)
sqc = SQLContext(sc)

def makeRDD(dstream):
	return dstream

lines = ssc.socketTextStream('localhost', 6100)
lines = lines.flatMap(lambda line: json.loads(line))
lines = lines.foreachRDD(makeRDD)

print('##################################################### printing words..')
print(type(lines))

ssc.start()
ssc.awaitTermination()
