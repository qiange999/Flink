package cn.mrt.flink.window;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import java.time.Duration;




//本文展示了如何使用flink窗口的方法，先keyby分区数据，再定义一个窗口，然后定义在窗口内的具体操作
public class WindowReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //加载数据源
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        //设置水位线参数
        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp);
        //获得带有水位线的数据
        SingleOutputStreamOperator<Event> streamOperator = stream.assignTimestampsAndWatermarks(watermarkStrategy);
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = streamOperator.map(new MapFunction<Event, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Event event) throws Exception {
                return new Tuple2<String, Integer>(event.user, 1);
            }
        });
        //分组并reduce;
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = map.keyBy(stringIntegerTuple2 -> stringIntegerTuple2.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of( Time.seconds(5)));
//      这种方式会报错 window不支持rich类
//        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = window.reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//            }
//
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
//                return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
//            }
//
//            @Override
//            public void close() throws Exception {
//                super.close();
//            }
//        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = window.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
            }
        });



        reduce.print();

        env.execute();
    }
}
