package com.flink.java.playground;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Kafka2Std {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(parameterTool.get("bootstrap.servers"))
                .setTopics(parameterTool.get("topic"))
                .setGroupId(parameterTool.get("group.id"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(parameterTool.getProperties())
                .build();
        RateLimiter rateLimiter = RateLimiter.create(10);
        DataStream<String> stream = environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map((MapFunction<String, String>) s -> {
                    rateLimiter.acquire(1);
            return s;
        });
        stream.print();
        environment.execute("Kafka 2 Print task");
    }
}
