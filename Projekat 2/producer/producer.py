import time
import os
import csv
import json
from datetime import datetime, timezone
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=[os.environ["KAFKA_HOST"]],
                         api_version=(0, 11))

print('Kafka message producer started')
while True:
    with open(os.environ["DATA"], "r") as file:
        reader = csv.reader(file, delimiter=";")
        headers = next(reader)
        for row in reader:
                emission_info = {}
                emission_info['timestep_time'] = float(row[0])
                emission_info['vehicle_CO'] = float(row[1]) if row[1] != "" else 0.0
                emission_info['vehicle_CO2'] = float(row[2]) if row[2] != "" else 0.0
                emission_info['vehicle_HC'] = float(row[3]) if row[3] != "" else 0.0
                emission_info['vehicle_NOx'] = float(row[4]) if row[4] != "" else 0.0
                emission_info['vehicle_PMx'] = float(row[5]) if row[5] != "" else 0.0
                emission_info['vehicle_angle'] = float(row[6]) if row[6] != "" else 0.0
                emission_info['vehicle_eclass'] = row[7]
                emission_info['vehicle_electricity'] = float(row[8]) if row[8] != "" else 0.0
                emission_info['vehicle_fuel'] = float(row[9]) if row[9] != "" else 0.0
                emission_info['vehicle_id'] = row[10]
                emission_info['vehicle_lane'] = row[11]
                emission_info['vehicle_noise'] = float(row[12]) if row[12] != "" else 0.0
                emission_info['vehicle_pos'] = float(row[13]) if row[13] != "" else 0.0
                emission_info['vehicle_route'] = row[14]
                emission_info['vehicle_speed'] = float(row[15]) if row[15] != "" else 0.0
                emission_info['vehicle_type'] = row[16]
                emission_info['vehicle_waiting'] = float(row[17]) if row[17] != "" else 0.0
                emission_info['vehicle_x'] = float(row[18]) if row[18] != "" else 0.0
                emission_info['vehicle_y'] = float(row[19]) if row[19] != "" else 0.0
                producer.send(os.environ["KAFKA_TOPIC"], value=json.dumps(emission_info).encode('utf-8'))
                producer.flush()
                print('Message sent', emission_info)
                time.sleep(0.1)

    print('Kafka message producer done')