package cn.mrt.flink.trans.agg;

import cn.mrt.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 * @author  cqh
 * @version 1.0
 */
public class TransKeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> DStream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));

        KeyedStream<Event, String> eventStringKeyedStream = DStream.keyBy(event -> event.user);
        eventStringKeyedStream.print();
        env.execute();
    }
}
