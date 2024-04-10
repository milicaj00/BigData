import time
import os
import csv
import json
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import pyspark
from dotenv import load_dotenv

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,FloatType
from pyspark.sql.functions import *

load_dotenv()


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_FCD = "test_topic_fcd"
KAFKA_TOPIC_EMISSION = "test_topic_emission"

dburl = 'influxdb' #os.getenv('dburl')
dbhost = '8086' #os.getenv('dbhost')
dbuser = 'milica' #os.getenv('dbuser')
dbpassword = 'milica123' #os.getenv('dbpassword')
token = 'QshhXmobSoOKdm6nJzQp5kuTLAz0ea7it7dNUBB--u-6TXyWXTZM-IR7ljEwTHiPeo1LqfrbtmFWCOZgmKH-1Q==' #os.getenv('token')
org = 'Big Data' #os.getenv('org')
bucket = 'Big Data' #os.getenv('bucket')


class InfluxDBWriter:
    def __init__(self):
        self.client =  InfluxDBClient(url = dburl, token=token, org=org)
        self.write_api = self.client.write_api()

    def open(self, partition_id, epoch_id):
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True

    def process(self, row):
        self.write_api.write(bucket=bucket, record=self._row_to_line_protocol(row))

    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))

    def _row_to_line_protocol(self, row: pyspark.sql.Row):
        # TODO map Row to LineProtocol
        return "measurement_name,tag_1=tag_1_value field_1=field_1_value 1"



if __name__ == "__main__":

    window_duration = "1 minute" #os.getenv('WINDOW_DURATION')
    N = 5

  
    vehicleSchema = StructType([
        StructField("Time", TimestampType()),
        StructField("vehicle_lane", StringType()),
        StructField("traffic_count", IntegerType())
    ])

    appName = "Stockholm2App"
    
    conf = SparkConf()
    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    dfFcd = spark \
        .readStream \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe",  KAFKA_TOPIC_FCD) \
        .load()
    
    #dfFcd.printSchema()
    
    dfFcdParsed = dfFcd.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), vehicleSchema).alias("data")).select("data.*")
    
    ########## BITNO

    # dfEmissionParsed = dfEmissionParsed.withColumn("Date", to_timestamp("Date", "MMM dd, yyyy, hh:mm:ss a"))
    dfFcdParsed = dfFcdParsed.withColumn("Date", to_timestamp("Date", "MMM dd, yyyy, hh:mm:ss a"))


    dfFcdParsed = dfFcdParsed.withColumnRenamed("Date", "date") \
                                .withColumnRenamed("LaneId", "laneId") \
                                .withColumnRenamed("VehicleCount", "vehicleCount")

    query_pollution2 = dfFcdParsed.writeStream \
        .outputMode("append") \
        .format("console") \
        .start().awaitTermination()

    # query_traffic = dfFcdParsed.writeStream.foreach(InfluxDBWriter())


    # print("PRINTING TRAFFIC DATA")
    # query_traffic2 = grouped_data_traffic.writeStream \
    # .outputMode("complete") \
    # .format("console") \
    # .option("truncate", "false") \
    # .start()


    spark.stop()   