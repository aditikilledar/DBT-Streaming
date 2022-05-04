from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from json import dumps

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

def send_to_kafka(rows):
    
        word = rows[0]
        count = rows[1]
        print(rows)
        print(type(word))
        producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'], api_version=(0, 10, 1), value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

        
        v= {rows[0]:rows[1]}
        if(word == 'NCT' or word == "BLACKPINK" or word == 'EXO' or word == 'SHINEE' or word == 'AESPA'):
            producer.send(word,value=v)
            print("$$$$$$$$$$$$$$$$$$$$$$$$TWEET RECIEVED$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
           

if __name__ == "__main__":
    
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 1)
    lines = ssc.socketTextStream("localhost", 5556)
    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    # count how mant pairs are there
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    wordCounts.foreachRDD(lambda rdd: rdd.foreach(send_to_kafka))
    ssc.start()             # Start the computation
    ssc.awaitTermination()
    

# def on_send_success(record_metadata):
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)


# def on_send_error(excp):
#     log.error('ERROR', exc_info=excp)
#     