SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'bigdata';

DROP KEYSPACE IF EXISTS bigdata;

CREATE KEYSPACE bigdata WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

USE bigdata;

DROP TABLE IF EXISTS bigdata.pollution;

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
);

DROP TABLE IF EXISTS bigdata.traffic;

CREATE TABLE bigdata.traffic (
            timestep_start_time text PRIMARY KEY,
            timestep_end_time text,
            vehicle_lane text,
            traffic_count int
);