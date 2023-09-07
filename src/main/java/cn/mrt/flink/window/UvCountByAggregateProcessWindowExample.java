package cn.mrt.flink.window;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import scala.tools.nsc.doc.model.Public;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class UvCountByAggregateProcessWindowExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> streamWithTimestamp = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestamp;
            }
        }));

        AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> UVDivedePvaggregateFunction = new AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double>() {
            @Override
            public Tuple2<HashSet<String>, Long> createAccumulator() {
                HashSet<String> strings = new HashSet<>();
                return new Tuple2<>(strings, 0L);
            }

            @Override
            public Tuple2<HashSet<String>, Long> add(Event event, Tuple2<HashSet<String>, Long> hashSetLongTuple2) {

                hashSetLongTuple2.f0.add(event.user);
                return Tuple2.of(hashSetLongTuple2.f0, hashSetLongTuple2.f1 + 1l);
            }

            @Override
            public Double getResult(Tuple2<HashSet<String>, Long> hashSetLongTuple2) {
                return Double.valueOf(hashSetLongTuple2.f0.size()) /Double.valueOf(hashSetLongTuple2.f1) ;
            }

            @Override
            public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
                return null;
            }
        };

        ProcessWindowFunction<Double, String, String, TimeWindow> processWindowFunction = new ProcessWindowFunction<Double, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<Double, String, String, TimeWindow>.Context context, Iterable<Double> elements, Collector<String> out) throws Exception {
                long start = context.window().getStart();
                long end = context.window().getEnd();

                out.collect("窗口开始位置：" + start + " , 结束位置： " + end + " UV/PV=" + elements);
            }
        };

        streamWithTimestamp.keyBy(event -> event.user)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(UVDivedePvaggregateFunction,processWindowFunction ).print();




        env.execute();

    }
}
