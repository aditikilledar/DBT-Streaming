"""ACCEPTS DATA STREAMED ON PORT 6100"""

# run as $SPARK_HOME/bin/spark-submit client.py 

import numpy as np
import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext, DStream
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.functions import from_json, col, when

# config
sc = SparkContext("local[2]", "NetworkWordCount")
#spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)
sqc = SQLContext(sc)
#testing
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

def makeRDD(dstream):
	#print('$$$$$$$$$$$$$$$$$$$$$$$$$$$inside makeRDD function')
	return dstream

#Id,Dates,DayOfWeek,PdDistrict,Address,X,Y

def jsonToDf(rdd):
	global bsize
	if not rdd.isEmpty():
		df = spark.read.json(rdd)
		batch_df = df.select(col("0.*")).withColumnRenamed('feature0','Sentiment').withColumnRenamed('feature1','Tweet')
		for row_no in range(1, bsize):
			s = str(row_no)+".*"
			df3 = df.select(col(s)).withColumnRenamed('feature0','Sentiment').withColumnRenamed('feature1','Tweet')
			batch_df = batch_df.union(df3)
		print(batch_df)
		return batch_df
	return None
		

lines = ssc.socketTextStream('localhost', 6100)
#lines = lines.flatMap(lambda line: json.loads(line))
#lines = lines.foreachRDD(makeRDD)
lines.foreachRDD(lambda rdd: jsonToDf(rdd))
#lines = jsonToDf(lines)



print('##################################################### printing words..')
print(lines)
print(type(lines))

ssc.start()
ssc.awaitTermination()

