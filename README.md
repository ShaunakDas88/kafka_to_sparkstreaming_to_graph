# kafka_to_sparkstreaming_to_graph

This Spark application loads data from a Kafka queue into a DseGraph instance, by way of SparkStreaming and DseGraphFrames APIs.

### Documentation
Apache Kafka: http://kafka.apache.org/documentation/

Apache Spark Streaming: https://spark.apache.org/docs/latest/streaming-programming-guide.html

Kafka + Spark Streaming Integration: https://spark.apache.org/docs/latest/streaming-kafka-integration.html

DSE GraphFrames: https://docs.datastax.com/en/dse/5.1/dse-dev/datastax_enterprise/graph/graphAnalytics/dseGraphFrameImport.html

### Getting Started
This project leverage sbt for Scala package management and building. In addition, one must have DSE Spark already running on their cluster. 

1. Clone the kafka_to_sparkstreaming_to_graph project to your cluster that is running DSE Spark, and build with sbt
```
git clone git@github.com:ShaunakDas88/kafka_to_sparkstreaming_to_graph.git
```
```
cd <PATH TO kafka_to_sparkstreaming_to_graph>
sbt package
```

2. Download and unpack the Amazon data set (http://jmcauley.ucsd.edu/data/amazon/links.html) to your DSE Spark cluster:
```
wget -P <DESTINATION DIRECTORY>
wget -P <DESTINATION DIRECTORY>
```

3. Unpack the Kafka project that is provided within this project
```
cd <PATH TO kafka_to_sparkstreaming_to_graph>/resources/kafka
tar -zxvf 
```

4. Launch the ZooKeeper server as a background process:

5. Launch the Kafka server as a backgorund process:

6. Create two topics, one for the Amazon metadata and one for the Amazon reviews data:

7. Launch the Kafka standalone application:

8.




