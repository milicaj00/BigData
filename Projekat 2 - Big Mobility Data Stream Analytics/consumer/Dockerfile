FROM bitnami/spark:latest

COPY requirements.txt /app/
RUN cd /app \
      && pip3 install -r requirements.txt

# Copy the source code
COPY . /app

ENV SPARK_MASTER_NAME spark-master-x
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/consumer.py
ENV CASSANDRA_HOST cassandra
ENV CASSANDRA_PORT 9042

ENV CASSANDRA_KEYSPACE bigdata
ENV POLLUTION_TABLE pollution
ENV TRAFFIC_TABLE traffic


ENV EMISSION_TOPIC "berlin-emission"
ENV FCD_TOPIC "berlin-fcd"
ENV POLLUTION_TOPIC "berlin-pollution"
ENV TRAFFIC_TOPIC "berlin-traffic"
ENV KAFKA_URL "kafka:9092"


ENV SPARK_SUBMIT_ARGS "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0"

COPY submit.sh /
CMD ["/bin/bash", "/submit.sh"]