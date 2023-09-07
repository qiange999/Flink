package cn.mrt.flink.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LinkedOptionalMap;
import sun.util.resources.ga.LocaleNames_ga;

/*
 * @author  cqh
 * @version 1.0
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //对比javaSparkContext(sparkConf) 同一个道理
        //2 通过env读取数据
        DataSource<String> stringDataSource = env.readTextFile("src/main/resources/wordcount.txt");
        //3 将数据转换为kv键值对
        FlatMapOperator<String, Tuple2<String ,Long>> flatMapData = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String ,Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String ,Long>> collector) throws Exception {
                String[] words = line.split(" ");
                //往words写入键值对
                for (String word : words) {
                    //此处不能够new一个Tuple2 否则会报runtime错
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //4 对单词进行分组(按照哪个字段分组)
        UnsortedGrouping<Tuple2<String ,Long>> flatMapDataUG = flatMapData.groupBy(0);
        //5 求和操作（求和哪个字段）
        AggregateOperator<Tuple2<String, Long>> sum = flatMapDataUG.sum(1);
        sum.print();
    }
}
