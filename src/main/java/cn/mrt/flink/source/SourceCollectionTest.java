package cn.mrt.flink.source;

import cn.mrt.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/*
 * @author  cqh
 * @version 1.0
 */
public class SourceCollectionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1 创建一个集合，包含Event的数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream1 = env.fromCollection(events);

        //2 也可以从元素中直接读取数据
        DataStreamSource<Event> stream2 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        stream1.print("fromCollection");
        stream2.print("fromElements");

        env.execute();
    }


}
