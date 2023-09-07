package cn.mrt.flink.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
 * @author  cqh
 * @version 1.00-   `
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据源

        DataStreamSource<String> lineDS = env.socketTextStream("192.168.56.101", 4893);
        //转换kv键值对
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
//        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = flatMapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//            @Override
//            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                return stringIntegerTuple2;
//            }
//        });
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = flatMapDS.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2StringKeyedStream.sum(1);

        sum.print();



        //执行
        env.execute();
    }
}
