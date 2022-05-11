A repository containing code and data for the mini-project using Apache Spark and Kafka Streaming as part of the course Database Technologies (UE19CS344) at PES University '23. 

TO EXECUTE -

Prerequisites:
  1. Spark
  2. small.csv
  3. client.py
  4. stream.py

In one terminal, navigate to the directory that contains stream.py, client.py and small.csv. Run the following command -
  
  $SPARK_HOME/bin/spark-submit stream.py -f small.csv -b 100
  

In another terminal, navigate to the directory that contains spark-submit (usually opt/spark/spark-3.1.2-bin-hadoop3.2/bin) and run the following command -
  
  spark-submit path-to-client-file
  

These commands stream data from small.csv that contains crime information on port 6100, with a batch size of 100.
