package com.at.rt.data.warehouse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class M {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(
                        new SourceFunction<String>() {
                            @Override
                            public void run(SourceContext<String> ctx) throws Exception {

                                while (true) {
                                    ctx.collect("1");
                                    TimeUnit.SECONDS.sleep(1);
                                }

                            }

                            @Override
                            public void cancel() {

                            }
                        }
                )
                .print();

        env.execute();

    }
}
