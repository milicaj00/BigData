from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,FloatType
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, StringType, TimestampType, IntegerType, DoubleType, StructField, StructType
import os
import argparse

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "test_topic"

KAFKA_TOPIC_FCD = "test_topic_fcd"
KAFKA_TOPIC_EMISSION = "test_topic_emission"


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process Kafka topic data with time windows.')
    parser.add_argument('--window_type', type=str, choices=['tumbling', 'sliding'], help='Type of window (tumbling or sliding)')
    parser.add_argument('--window_duration', type=str, help='Duration of the window')
    parser.add_argument('--slide_duration', type=str, help='Duration of sliding for sliding window')

    args = parser.parse_args()

    print('args', args)

    if args.window_type == 'sliding':
       if args.slide_duration is None or args.window_duration is None:
          parser.error("For sliding window, both slide_duration and window_duration must be provided.")
       window_duration = args.window_duration 
       slide_duration = args.slide_duration
    elif args.window_type == 'tumbling':
       if args.window_duration is None:
           parser.error("For tumbling window, window_duration must be provided.")
       window_duration = "2 minutes"
       slide_duration = None  
    else:
        parser.error("Invalid window type. Please choose 'tumbling' or 'sliding'.")

    window_duration = '5 minutes'
    slide_duration = None

    appName = "BerlinApp"
    conf = SparkConf()
    conf.setMaster("local")
    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe",  KAFKA_TOPIC) \
        .load()
   

    dataSchema = StructType([
        StructField("timestep_time", FloatType()),
        StructField("vehicle_CO", FloatType()),
        StructField("vehicle_CO2", FloatType()),
        StructField("vehicle_HC", FloatType()),
        StructField("vehicle_NOx", FloatType()),
        StructField("vehicle_PMx", FloatType()),
        StructField("vehicle_angle", FloatType()),
        StructField("vehicle_eclass", StringType()),
        StructField("vehicle_electricity", FloatType()),
        StructField("vehicle_id", IntegerType()),
        StructField("vehicle_lane", StringType()),
        StructField("vehicle_fuel", FloatType()),
        StructField("vehicle_noise", FloatType()),
        StructField("vehicle_pos", FloatType()),
        StructField("vehicle_route", StringType()),
        StructField("vehicle_speed", FloatType()),
        StructField("vehicle_type", StringType()),
        StructField("vehicle_waiting", FloatType()),
        StructField("vehicle_x", FloatType()),
        StructField("vehicle_y", FloatType())
    ])

    df.printSchema()

    dfParsed = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), dataSchema).alias("data")).select("data.*")

    dfParsed = dfParsed.withColumn("timestep_time", to_timestamp(dfParsed['timestep_time'])) 

    pollution_data = dfParsed.groupBy(window("timestep_time", window_duration, slide_duration), col("vehicle_lane")) \
        .agg(
            avg("vehicle_CO").alias("vehicle_co"),
            avg("vehicle_CO2").alias("vehicle_co2"),
            avg("vehicle_HC").alias("vehicle_hc"),
            avg("vehicle_NOx").alias("vehicle_nox"),
            avg("vehicle_PMx").alias("vehicle_pmx"),
            avg("vehicle_noise").alias("vehicle_noise")
        )

    pollution_data = pollution_data.withColumn("timestep_start_time", pollution_data.window.start.cast("string")) 
    pollution_data = pollution_data.withColumn("timestep_end_time", pollution_data.window.end.cast("string")) 


   

    # TRAFFIC DATA

    traffic_count = dfParsed.groupBy(window("timestep_time", window_duration, slide_duration),col("vehicle_lane")).agg(count("*").alias("traffic_count"))

    traffic_count = traffic_count.withColumn("timestep_start_time", traffic_count.window.start.cast("string")) 
    traffic_count = traffic_count.withColumn("timestep_end_time", traffic_count.window.end.cast("string"))

    most_traffic_lanes = traffic_count.orderBy(col("traffic_count").desc())

    print("SENDING DATA")
    send_traffic = most_traffic_lanes \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC_FCD) \
        .option("checkpointLocation","checkpoint_dir2") \
        .outputMode("complete") \
        .start()

    print("PRINTING MOST TRAFFIC LANES")
    print_traffic = most_traffic_lanes.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()




    print("SENDING POLLUTION DATA")
    send_pollution = pollution_data \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC_EMISSION) \
        .option("checkpointLocation","checkpoint_dir2") \
        .outputMode("complete") \
        .start()

    print("PRINTING POLLUTION DATA")
    print_pollution = pollution_data.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()



    send_pollution.awaitTermination()
    print_pollution.awaitTermination()

    send_traffic.awaitTermination()
    print_traffic.awaitTermination()


    spark.streams.awaitAnyTermination()