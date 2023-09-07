package cn.mrt.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 * @author  cqh
 * @version 1.0
 */
public class SourceSocketTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.56.101", 4893);
        //DataStreamSource<String> stream1 = env.readTextFile("src/main/resources/input/clicks.csv");
        dataStreamSource.print();
        env.execute();
    }
}
