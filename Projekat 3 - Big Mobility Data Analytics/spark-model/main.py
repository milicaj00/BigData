import os
import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator, ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import RandomForestRegressor

emission_file = 'file:///app/emissions.csv'
data_location = 'hdfs://namenode:9000/data/data.csv'
save_kmeans_model = 'hdfs://namenode:9000/models/kmodel'
save_rf_model = 'hdfs://namenode:9000/models/rmodel'

if __name__ == '__main__':
    if len(sys.argv) < 1:
        print("Usage: main.py <input folder> ")
        exit(-1)

    spark = SparkSession.builder \
        .appName("ModelCreation") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    data = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://namenode:9000/dir/data.csv", inferSchema=True)
    data=data.limit(1000)
    data.show(10)

    (trainData, testData) = data.randomSplit([0.7, 0.3], seed=42)


    fcd_features = ["vehicle_x", "vehicle_y"]
    kmeans_assembler = VectorAssembler(inputCols=fcd_features, outputCol="fcd_features")
  
    kmeans = KMeans(k=4, seed=1, featuresCol="fcd_features", predictionCol="congestion_cluster")
    kmeans_pipeline = Pipeline(stages=[kmeans_assembler, kmeans])
    kmeans_model = kmeans_pipeline.fit(trainData)
    predictions2 = kmeans_model.transform(testData)

    kmeans_model.write().overwrite().save(save_kmeans_model)


    emission_features = ["vehicle_fuel", "vehicle_speed", "vehicle_noise"]
    regression_assembler = VectorAssembler(inputCols=emission_features, outputCol="emission_features")
 
    rf = RandomForestRegressor(featuresCol='emission_features', labelCol='vehicle_NOx', maxDepth=10)
    rf_pipeline = Pipeline(stages=[regression_assembler, rf])
    rf_model = rf_pipeline.fit(trainData)
    predictions = rf_model.transform(testData)

    rf_model.write().overwrite().save(save_rf_model)

    # Evaluacija regresora

    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="vehicle_NOx", metricName="mse")
    accuracy = evaluator.evaluate(predictions)
    print("Mean Squared error modela: ", accuracy)

    f1_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="vehicle_NOx", metricName="rmse")
    f1_score = f1_evaluator.evaluate(predictions)
    print("Root Mean Squared error modela: ", f1_score)

    recall_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="vehicle_NOx", metricName="mae")
    recall = recall_evaluator.evaluate(predictions)
    print("Mean Absolute error modela: ", recall)

    # Evaluacija algoritma za klasterizaciju

    evaluator = ClusteringEvaluator(predictionCol="congestion_cluster", featuresCol="fcd_features", metricName="silhouette")
    silhouette = evaluator.evaluate(predictions2)
    print("Silhouette Score:", silhouette)


    spark.stop()
