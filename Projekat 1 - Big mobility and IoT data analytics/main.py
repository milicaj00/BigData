import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sin, cos, radians, min, max, mean, stddev, avg, count, lit, acos

SPARK_URL = "spark://spark-master:7077"

def find_radius(data, lat, log, radius):

    target_latitude = radians(lit(lat))
    target_longitude = radians(lit(log))

    return data.filter((6371 * acos(
            sin(target_latitude) * sin(radians(data.vehicle_y)) + 
            cos(target_latitude) * cos(radians(data.vehicle_y)) * cos(radians(data.vehicle_x) - target_longitude)
        )) < radius).dropDuplicates(["vehicle_id"])
    
    
def find_vehicles(spark,  lat, log, radius, time_start, time_end, vehicle_type = ''):
    
    start_time = time.time()
    data = spark.read.option("header", "true").option("delimiter", ";").csv('hdfs://namenode:9000/data/b_fcd.csv', inferSchema=True)

    data_by_time = data.filter((data.timestep_time >= time_start) & (data.timestep_time <= time_end) & (data.vehicle_type.contains(vehicle_type)))
    data_final = find_radius(data_by_time,  lat, log, radius)
    
    print(f"Broj jedinstvenih vozila u okolini: {data_final.count()}")  

    data_final.show()

    return time.time() - start_time


def air_polution(spark, time_start, time_end):
    
    start_time = time.time()
    data = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://namenode:9000/data/b_emissions.csv", inferSchema=True)
    data_by_time = data.filter((data.timestep_time >= time_start) & (data.timestep_time <= time_end))

    for col_name in ["vehicle_CO", "vehicle_CO2", "vehicle_HC", "vehicle_NOx", "vehicle_PMx", "vehicle_noise", "vehicle_electricity", "vehicle_fuel"]:
        data_by_time.groupBy("vehicle_lane").agg(
                count(col(col_name)).alias(f"count_{col_name}"),
                min(col(col_name)).alias(f"min_{col_name}"), 
                max(col(col_name)).alias(f"max_{col_name}"), 
                mean(col(col_name)).alias(f"mean_{col_name}"), 
                avg(col(col_name)).alias(f"avg_{col_name}"), 
                stddev(col(col_name)).alias(f"sddev_{col_name}") 
            ).show()

    return time.time() - start_time

def fuel_consumption(spark, time_start, time_end):
    
    start_time = time.time()
    data = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://namenode:9000/data/b_emissions.csv", inferSchema=True)
    data_by_time = data.filter((data.timestep_time >= time_start) & (data.timestep_time <= time_end))

    for col_name in ["vehicle_electricity", "vehicle_fuel"]:
        data_by_time.groupBy("vehicle_lane").agg(
                count(col(col_name)).alias(f"count_{col_name}"),
                min(col(col_name)).alias(f"min_{col_name}"), 
                max(col(col_name)).alias(f"max_{col_name}"), 
                mean(col(col_name)).alias(f"mean_{col_name}"), 
                avg(col(col_name)).alias(f"avg_{col_name}"), 
                stddev(col(col_name)).alias(f"sddev_{col_name}") 
            ).show()

    return time.time() - start_time

if __name__ == "__main__":

    args = sys.argv
    print(args)

    app_name = "App/"+ args[1]
    spark_master = "local[2]" if args[2] == 'local' else SPARK_URL
    start_time = time.time()
    spark = SparkSession.builder.appName(app_name).master(SPARK_URL).getOrCreate()
    task_time = 0

    if args[3] == 'vehicle':
        if(len(args) < 9):
            print("Nedovoljno argumenata!")
            exit()
        print("finding vehicles...")
        vehicle_type = args[9] if len(args) == 9 else ''

        task_time = find_vehicles(spark, float(args[4]),float(args[5]),float(args[6]), float(args[7]), float(args[8]), vehicle_type)
        
    elif args[3] == 'pollution':
        print("calculating air pollution...")

        if(len(args) < 6):
            print("Nedovoljno argumenata!")
            exit()

        task_time = air_polution(spark, float(args[4]), float(args[5]))

    elif args[3] == 'fuel':
        print("calculating fuel consumption...")
        if(len(args) < 6):
            print("Nedovoljno argumenata!")
            exit()

        task_time = fuel_consumption(spark, float(args[4]), float(args[5]))


    print(f"Vreme izvrsenja taska {args[1]}: {task_time}")
    print(f"Vreme izvrsenja aplikacije: {time.time() - start_time}")
    spark.stop()
    exit()