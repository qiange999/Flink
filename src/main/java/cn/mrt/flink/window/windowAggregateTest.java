package cn.mrt.flink.window;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;
import java.util.HashSet;

public class windowAggregateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        WatermarkStrategy<Event> eventWatermarkStrategy = WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                });
        SingleOutputStreamOperator<Event> streamOperator = stream.assignTimestampsAndWatermarks(eventWatermarkStrategy);
        WindowedStream<Event, String, TimeWindow> windowWindowedStream = streamOperator.keyBy(event -> event.user).window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(2)));
        SingleOutputStreamOperator<Double> aggregatestream = windowWindowedStream.aggregate(new AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double>() {
            @Override
            public Tuple2<HashSet<String>, Long> createAccumulator() {
                return Tuple2.of(new HashSet<String>(), 0L);
            }

            @Override
            public Tuple2<HashSet<String>, Long> add(Event event, Tuple2<HashSet<String>, Long> hashSetLongTuple2) {
                hashSetLongTuple2.f0.add(event.user);
                return Tuple2.of(hashSetLongTuple2.f0, hashSetLongTuple2.f1 + 1L);
            }

            @Override
            public Double getResult(Tuple2<HashSet<String>, Long> hashSetLongTuple2) {
                return (double) (hashSetLongTuple2.f1 / hashSetLongTuple2.f0.size());
            }

            @Override
            public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
                return null;
            }
        });



        aggregatestream.print();

        ///使用聚合方法

        env.execute();
    }
}
