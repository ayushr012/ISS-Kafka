# Kafka Spark Streaming: ISS Location Tracking

This project streams real-time ISS (International Space Station) location data using Apache Kafka and processes it using Apache Spark Structured Streaming.

## Project Overview:

Producer: Fetches ISS location data from the Open Notify API and sends it to a Kafka topic.

Kafka: Acts as a message broker to store and distribute streaming data.

Spark Structured Streaming: Reads data from Kafka, processes it, and saves it in CSV format.

## Prerequisites:

**Make sure you have the following installed:**

Python 3.x

Apache Kafka

Apache Spark (3.x recommended)

Zookeeper (for Kafka)


**Python dependencies: requests, pyspark, findspark**

    Installation :

          1. Install Required Python Libraries

                pip install requests findspark pyspark

          2. Start Zookeeper and Kafka

                Open a terminal and start Zookeeper:

                        zookeeper-server-start.sh yourconfig/zookeeper.properties

                Start Kafka server:

                        kafka-server-start.sh yourconfig/server.properties

          3. Create Kafka Topic

                kafka-topics.sh --create --topic testtopic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

**Running the Project:**

          1. Run Kafka Producer

                  This script fetches ISS location data and sends it to Kafka.

                              python producer.py

          2. Run Spark Streaming Consumer

                  This script reads from Kafka, processes data, and saves it in CSV format.

                              spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1 consumer.py


## Console Output:

**Expected Output:**

+---------+----------+---------+------------+
|latitude |longitude |message  |timestamp  |
+---------+----------+---------+------------+
| 17.3074 | 66.2531 | success | 1739357176 |
+---------+----------+---------+------------+







