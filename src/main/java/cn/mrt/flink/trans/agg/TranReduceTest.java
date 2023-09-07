package cn.mrt.flink.trans.agg;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

/*
 * @author  cqh
 * @version 1.0
 */
public class TranReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Tuple2<String, Long>> userStream = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return new Tuple2<>(event.user, 1L);
            }
        });
//        //分区
//        userStream.keyBy(tuple -> tuple.f0).sum(1).print();
        KeyedStream<Tuple2<String, Long>, Boolean> userKeyByStream = userStream.keyBy(t -> true);
        //userKeyByStream.print();

        SingleOutputStreamOperator<Tuple2<String, Long>> userCountStream     = userStream.keyBy(t -> t.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                System.out.println("上次处理结果" + t1);
                System.out.println("当前到来的数据" + t2);
                long sum = t1.f1 + t2.f1;
                return new Tuple2<>(t1.f0, sum);
            }
        });
        userCountStream.print();
        //所有的分区一起统计
//        SingleOutputStreamOperator<Tuple2<String, Long>> reduceStream = userKeyByStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
//            @Override
//            public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
//                long sum = t1.f1 + t2.f1;
//                return new Tuple2<>(t1.f0, sum);
//            }
//        });
//
//        reduceStream.print();
        env.execute();
    }
}
