package cn.mrt.flink.trans.phypart;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

/*
 * @author  cqh
 * @version 1.0
 */
public class CustomPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> countstream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        countstream.partitionCustom(new Partitioner<Integer>() {
            //partition方法的两个参数：
            //第一个：是keyselector返回的值，
            //第二个：返回值：结合数据流算子的并行度，到底怎么分区的参数。例如如果我们想要按照奇数偶数分区的话，返回值就直接取除余2。
            @Override
            public int partition(Integer integer, int partitions) {
                return integer % 2;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer integer) throws Exception {
                return integer;
            }
        }).print().setParallelism(2);
        env.execute();
    }
}
