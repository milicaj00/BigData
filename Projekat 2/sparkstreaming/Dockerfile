FROM bde2020/spark-python-template:3.1.2-hadoop3.2

ENV KAFKA_HOST=kafka:9092
ENV KAFKA_TOPIC=munich-em
ENV KAFKA_CONSUMER_GROUP=Spark-Group
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/sparkstreaming.py
ENV SPARK_APPLICATION_ARGS --window_type sliding --window_duration 5minutes --slide_duration 10
ENV SPARK_SUBMIT_ARGS --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 