import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator, ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import RandomForestRegressor

emission_file = 'file:///app/emissions.csv'
data_location = 'hdfs://namenode:9000/dir/data.csv'
save_kmeans_model = 'hdfs://namenode:9000/models/kmodel'
save_rf_model = 'hdfs://namenode:9000/models/rmodel'

if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName("ModelCreation") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    data = spark.read.option("header", "true").option("delimiter", ";").csv(data_location, inferSchema=True)
    data=data.limit(1000)
    data.show(10)

    (trainData, testData) = data.randomSplit([0.7, 0.3], seed=42)

# clastering
    fcd_features = ["vehicle_x", "vehicle_y"]
    kmeans_assembler = VectorAssembler(inputCols=fcd_features, outputCol="fcd_features")
  
    kmeans = KMeans(k=4, seed=1, featuresCol="fcd_features", predictionCol="congestion_cluster")
    kmeans_pipeline = Pipeline(stages=[kmeans_assembler, kmeans])
    kmeans_model = kmeans_pipeline.fit(trainData)
    kmeans_pred = kmeans_model.transform(testData)

    kmeans_model.write().overwrite().save(save_kmeans_model)

    evaluator = ClusteringEvaluator(predictionCol="congestion_cluster", featuresCol="fcd_features", metricName="silhouette")
    silhouette = evaluator.evaluate(kmeans_pred)
    print("Silhouette Score:", silhouette)

# regresion
    emission_features = ["vehicle_fuel", "vehicle_speed", "vehicle_noise"]
    regression_assembler = VectorAssembler(inputCols=emission_features, outputCol="emission_features")

    rf = RandomForestRegressor(featuresCol='emission_features', labelCol='vehicle_NOx', maxDepth=20)
    rf_pipeline = Pipeline(stages=[regression_assembler, rf])
    rf_model = rf_pipeline.fit(trainData)
    predictions = rf_model.transform(testData)

    rf_model.write().overwrite().save(save_rf_model)

    recall_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="vehicle_NOx", metricName="mae")
    recall = recall_evaluator.evaluate(predictions)
    print("Mean Absolute error modela: ", recall)


    spark.stop()


    # for n in [2,3,4,5,6,7,8,9]:
    #     kmeans = KMeans(k=n, seed=1, featuresCol="fcd_features", predictionCol="congestion_cluster")
    #     kmeans_pipeline = Pipeline(stages=[kmeans_assembler, kmeans])
    #     kmeans_model = kmeans_pipeline.fit(trainData)
    #     predictions2 = kmeans_model.transform(testData)

    #     evaluator = ClusteringEvaluator(predictionCol="congestion_cluster", featuresCol="fcd_features", metricName="silhouette")
    #     silhouette = evaluator.evaluate(predictions2)

    #     print("Silhouette Score:", n, silhouette)

     
    # for n in [10, 15, 20, 30]:
    #     rf = RandomForestRegressor(featuresCol='emission_features', labelCol='vehicle_NOx', maxDepth=n)
    #     rf_pipeline = Pipeline(stages=[regression_assembler, rf])
    #     rf_model = rf_pipeline.fit(trainData)
    #     predictions = rf_model.transform(testData)

    #     f1_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="vehicle_NOx", metricName="rmse")
    #     f1_score = f1_evaluator.evaluate(predictions)

    #     recall_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="vehicle_NOx", metricName="mae")
    #     recall = recall_evaluator.evaluate(predictions)

    #     print("n, rmse, mae", n,f1_score, recall)