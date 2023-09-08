package cn.mrt.flink.stream;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class ConnectCoProcessTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> streamSource1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> streamSource2 = env.fromElements(1L, 2L, 3L);
        ConnectedStreams<Integer, Long> connect = streamSource1.connect(streamSource2);
        connect.process(new CoProcessFunction<Integer, Long, Object>() {
            @Override
            public void processElement1(Integer value, CoProcessFunction<Integer, Long, Object>.Context ctx, Collector<Object> out) throws Exception {
                out.collect(value + "integer");
            }

            @Override
            public void processElement2(Long value, CoProcessFunction<Integer, Long, Object>.Context ctx, Collector<Object> out) throws Exception {
                out.collect(value + "Long");
            }
        }).print();
        env.execute();
    }
}
