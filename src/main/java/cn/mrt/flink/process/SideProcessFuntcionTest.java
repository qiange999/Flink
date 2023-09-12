package cn.mrt.flink.process;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SideProcessFuntcionTest {

    private static final OutputTag<Tuple3<String,String, Long>> MaryTag =
            new OutputTag<Tuple3<String,String, Long>>("Mary-pv"){};
    private static final OutputTag<Tuple3<String,String, Long>> BobTag =
            new OutputTag<Tuple3<String,String, Long>>("Bob-pv"){};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> wmStream = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));
        SingleOutputStreamOperator<Event> processStream = wmStream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {
                if (event.user == "Mary") {
                    context.output(MaryTag, new Tuple3<>(event.user, event.user, event.timestamp));
                } else if (event.user == "Bob") {
                    context.output(BobTag, new Tuple3<>(event.user, event.user, event.timestamp));
                } else {
                    collector.collect(event);
                }
            }
        });
        processStream.getSideOutput(MaryTag).print("mary");
        processStream.getSideOutput(BobTag).print("bob");

        processStream.print("else");
        env.execute();
    }
}
