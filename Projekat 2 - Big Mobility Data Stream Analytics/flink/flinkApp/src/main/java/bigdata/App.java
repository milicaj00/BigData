package bigdata;

import java.util.Properties;
import bigdata.deserializers.*;
import bigdata.pojo.*;
import bigdata.analytics.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.Date;

public class App {

    static String kafka_url = "kafka:9092";
    static String emission_topic = "berlin-emission";
    static String pollution_topic = "berlin-pollution";
    static String fcd_topic = "berlin-fcd";
    static String traffic_topic = "berlin-traffic";

    static String window_type = "tumbling";
    static int window_duration = 2;
    static int slide_duration = 30;
    
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DeserializationSchema<VehicleInfo> vehicleSchema = new VehicleInfoDeserializer();
        final DeserializationSchema<EmissionInfo> emissionSchema = new EmissionInfoDeserializer();


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafka_url);

        KafkaSource<VehicleInfo> vehicleInfoKafkaSource = KafkaSource.<VehicleInfo>builder()
                .setBootstrapServers(kafka_url)
                .setTopics(fcd_topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(vehicleSchema))
                .build();

        KafkaSource<EmissionInfo> emissionInfoKafkaSource = KafkaSource.<EmissionInfo>builder()
                .setBootstrapServers(kafka_url)
                .setTopics(emission_topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(emissionSchema))
                .build();

        DataStream<EmissionInfo> emissionInfoDataStream = env.fromSource(emissionInfoKafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "emi-source")
                .filter(new FilterFunction<EmissionInfo>() {
                    @Override
                    public boolean filter(EmissionInfo emissionInfo) throws Exception {
                        return emissionInfo.VehicleLane != null;
                    }
                });

        DataStream<VehicleInfo> vehicleInfoDataStream = env.fromSource(vehicleInfoKafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "veh-source")
                .filter(new FilterFunction<VehicleInfo>() {
                    @Override
                    public boolean filter(VehicleInfo vehicleInfo) throws Exception {
                        return vehicleInfo.VehicleLane != null && vehicleInfo.VehicleId != null;
                    }
                });


        // --------------------------------------------------------------------------------------------------------------------

        WindowedStream<VehicleInfo, String, TimeWindow> laneGroupedWindowedStream;
        WindowedStream<EmissionInfo, String, TimeWindow> laneGroupedWindowedStream2;
        
        if (window_type == "tumbling") {
            laneGroupedWindowedStream = vehicleInfoDataStream
                .keyBy(VehicleInfo::getVehicleLane)
                .window(TumblingEventTimeWindows.of(Time.minutes(window_duration)));

            laneGroupedWindowedStream2 = emissionInfoDataStream
                .keyBy(EmissionInfo::getVehicleLane)
                .window(TumblingEventTimeWindows.of(Time.minutes(window_duration)));

        } else {
            laneGroupedWindowedStream = vehicleInfoDataStream
                .keyBy(VehicleInfo::getVehicleLane)
                .window(SlidingEventTimeWindows.of(Time.minutes(window_duration), Time.seconds(slide_duration)));
        
            laneGroupedWindowedStream2 = emissionInfoDataStream
                .keyBy(EmissionInfo::getVehicleLane)
                .window(SlidingEventTimeWindows.of(Time.minutes(window_duration), Time.seconds(slide_duration)));
        
        }

        //pollution
        DataStream<PollutionData> laneEmissions = LaneAnalyzer.laneAggregation(laneGroupedWindowedStream2);
        laneEmissions.addSink(new FlinkKafkaProducer<PollutionData>(pollution_topic, new PollutionDataSerializer(), properties));

        //traffic
        DataStream<TrafficData> vehicleCount = LaneAnalyzer.calculateVehiclesOnLane(laneGroupedWindowedStream);
        vehicleCount.addSink(new FlinkKafkaProducer<TrafficData>(traffic_topic, new TrafficDataSerializer(), properties));

        env.execute("Berlin");
    }

}
