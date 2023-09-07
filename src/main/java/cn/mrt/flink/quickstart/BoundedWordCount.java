package cn.mrt.flink.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
 * @author  cqh
 * @version 1.0
 */
public class BoundedWordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //对比javaSparkContext(sparkConf) 同一个道理
        //2 通过env读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("src/main/resources/wordcount.txt");
        //3 将数据转换为kv键值对
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapData = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word,1));
                }
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT));


//        //4 对单词进行分组(按照哪个字段分组)
//        UnsortedGrouping<Tuple2<String ,Long>> flatMapDataUG = flatMapData.groupBy(0);
        //keyby方法
        //这里的第二个string是重写的返回值
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = flatMapData.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        //5 求和操作（求和哪个字段）
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2StringKeyedStream.sum(1);

        sum.print();
        //类似spark，必须要一个类似执行算子的执行
        env.execute();



    }
}
