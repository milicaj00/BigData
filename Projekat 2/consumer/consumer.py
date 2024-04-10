from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,FloatType
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, StringType, TimestampType, IntegerType, DoubleType, StructField, StructType
import argparse
from datetime import datetime, timezone
from datetime import datetime
import pyspark
from dotenv import load_dotenv
import json
import os
from cassandra.cluster import Cluster

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "test_topic"
KAFKA_TOPIC_FCD = "test_topic_fcd"
KAFKA_TOPIC_EMISSION = "test_topic_emission"

keyspace = "bigdata"
pollution_table = "pollution"
traffic_table = "traffic"


def writePollutionToCassandra(writeDF, epochId):
    print("Writing to Cassandra")
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=pollution_table, keyspace=keyspace) \
        .save()
    print("Data written to Cassandra for pollution table", epochId)

def writeTrafficToCassandra(writeDF, epochId):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=traffic_table, keyspace=keyspace) \
        .save()
    print("Data written to Cassandra for traffic table", epochId)
    
def create_database(cassandra_session):
    print("CREATE DATABASE")

    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdata
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
        """)

    cassandra_session.set_keyspace(keyspace)

    cassandra_session.execute("DROP TABLE IF EXISTS bigdata.pollution")
    cassandra_session.execute("""
        CREATE TABLE bigdata.pollution (
            timestep_start_time text PRIMARY KEY,
            timestep_end_time text,
            vehicle_CO text,
            vehicle_CO2 float,
            vehicle_HC float,
            vehicle_NOx float,
            vehicle_PMx float,
            vehicle_noise float,
            vehicle_lane text
        )
    """)
    
    cassandra_session.execute("DROP TABLE IF EXISTS bigdata.traffic")
    cassandra_session.execute("""
        CREATE TABLE bigdata.traffic (
            timestep_start_time text PRIMARY KEY,
            timestep_end_time text,
            vehicle_lane text,
            traffic_count int
        )
    """)

if __name__ == "__main__":

    cassandra_host = "cassandra" #os.getenv('CASSANDRA_HOST')
    cassandra_port = 9042 #int(os.getenv('CASSANDRA_PORT'))
    kafka_url = "kafka:9092" #os.getenv('KAFKA_URL')

    cassandra_cluster = Cluster([cassandra_host], port=cassandra_port)
    cassandra_session = cassandra_cluster.connect()
    create_database(cassandra_session)


    appName = "BerlinApp2"
    conf = SparkConf()

    conf.set("spark.cassandra.connection.host", cassandra_host)
    conf.set("spark.cassandra.connection.port", cassandra_port)

    conf.setMaster("local")
    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dfEmission = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe",  KAFKA_TOPIC_EMISSION) \
        .load()
      
    emissionSchema = StructType([
        StructField("vehicle_co", FloatType()),
        StructField("vehicle_co2", FloatType()),
        StructField("vehicle_hc", FloatType()),
        StructField("vehicle_nox", FloatType()),
        StructField("vehicle_pmx", FloatType()),
        StructField("vehicle_noise", FloatType()),
        StructField("vehicle_lane", StringType()),
        StructField("timestep_start_time", StringType()),
        StructField("timestep_end_time", StringType()),
    ])


    dfEmission.printSchema()

    dfEmissionParsed = dfEmission.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), emissionSchema).alias("data")).select("data.*")


## TRAFFIC
    dfTraffic = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe",  KAFKA_TOPIC_FCD) \
        .load()

    trafficSchema = StructType([
        StructField("traffic_count", IntegerType()),
        StructField("vehicle_lane", StringType()),
        StructField("timestep_start_time", StringType()),
        StructField("timestep_end_time", StringType()),
    ])


    dfTraffic.printSchema()

    dfTrafficParsed = dfTraffic.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), trafficSchema).alias("data")).select("data.*")



    print_pollution = dfEmissionParsed.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    query_pollution = dfEmissionParsed.writeStream \
        .option("spark.cassandra.connection.host","cassandra:9042")\
        .foreachBatch(writePollutionToCassandra) \
        .outputMode("append") \
        .start()
    
   
    print_traffic = dfTrafficParsed.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    write_traffic = dfTrafficParsed.writeStream \
        .option("spark.cassandra.connection.host","cassandra:9042")\
        .foreachBatch(writeTrafficToCassandra) \
        .outputMode("append") \
        .start()
    
    print_traffic.awaitTermination()
    write_traffic.awaitTermination()

    print_pollution.awaitTermination()
    query_pollution.awaitTermination()

    spark.streams.awaitAnyTermination()