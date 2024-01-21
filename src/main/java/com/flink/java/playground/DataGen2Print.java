package com.flink.java.playground;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

public class DataGen2Print {
    public static void main(String[] args) throws Exception {
        for(String arg : args) {
            System.out.println("custom args: " + arg);
        }
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataGeneratorSource source = new DataGeneratorSource<>(RandomGenerator.intGenerator(0, 1000000), 1, null);
        DataStream<String> stringDataStream = environment.addSource(source, "Generate source")
                .returns(Types.INT)
                .filter((FilterFunction<Integer>) num -> num %2 == 0 )
                .map((MapFunction<Integer, String>) num -> "test num: " + num);
        stringDataStream.print();
        environment.execute("DataGen 2 Print Task");
    }
}
