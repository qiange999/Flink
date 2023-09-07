package cn.mrt.flink.trans.agg;

import cn.mrt.flink.pojo.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 * @author  cqh
 * @version 1.0
 */
public class TransTupleAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> tuple2dStream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("a", 2),
                Tuple2.of("b", 3),
                Tuple2.of("b",4));
//        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2DataStreamSource.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//            @Override
//            public String getKey(Tuple2<String, Integer> Tuple2) throws Exception {
//                return Tuple2.f0;
//            }
//        });
        //KeyedStream<Tuple2<String, Integer>, String> keyedStream = tuple2DataStreamSource.keyBy(t -> t.f0);
        //SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        //sum.print();
//        tuple2DStream.keyBy(t->t.f0).sum(1).print();
        //KeyedStream<Tuple2<String, Integer>, String> keyedStream = tuple2dStream.keyBy(t -> t.f0);
//        tuple2dStream.keyBy(t -> t.f0).sum("f1").print();
//        tuple2dStream.keyBy(t -> t.f0).sum(1).print();
//        tuple2dStream.keyBy(t -> t.f0).max(1).print();
//        tuple2dStream.keyBy(t -> t.f0).max("f1").print();
//        tuple2dStream.keyBy(t -> t.f0).min(1).print();
//        tuple2dStream.keyBy(t -> t.f0).min("f1").print();
        tuple2dStream.keyBy(t -> t.f0).maxBy(1).print();
        tuple2dStream.keyBy(t -> t.f0).maxBy("f1").print();
        tuple2dStream.keyBy(t -> t.f0).minBy(1).print();
        tuple2dStream.keyBy(t -> t.f0).minBy("f1").print();
        env.execute();
    }
}
