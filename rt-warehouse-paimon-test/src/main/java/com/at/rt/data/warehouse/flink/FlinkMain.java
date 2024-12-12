package com.at.rt.data.warehouse.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkMain {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello, Flink!");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        while (true) {
                            ctx.collect("Hello, Flink!");
                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .print();

        env.execute("Flink Streaming Example");
    }
}
