import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import time

if __name__=="__main__":
    
    #sc=SparkContext(appname="Kafka Spark Demo")
    
    spark=SparkSession.builder.master("local").appname("Kafka Spark Demo") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1") \
    .getOrCreate()
    
    sc=spark.sparkContext
    
    ssc=StreamingContext(sc,20)
    
    message=KafkaUtils.createDirectStream(ssc,topics=['testtopic'],kafkaParams={"metadata.broker.list":"localhost:9092"})
    
    #data is kafka streamed RDD
    data=message.map(lambda x: x[1])
    
    def functordd(rdd):
        try:
            rdd1=rdd.map(lambda x: json.loads(x))
            df=spark.read.json(rdd1)
            df.show()
            df.createOrReplaceTempView("Test")
            df1=spark.sql("select iss_position.latitude,iss_position.longitude,message,timestamp from Test")
        
            df1.write.format('csv').mode('append').save("testing")
        
        except:
            pass
    
    data.foreachRDD(functordd)
    
    ssc.start()
    ssc.awaitTermination()
    
