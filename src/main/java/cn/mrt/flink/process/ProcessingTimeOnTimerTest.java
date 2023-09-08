package cn.mrt.flink.process;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;

public class ProcessingTimeOnTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> watermarksStream = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestamp;
            }
        }));

        watermarksStream.keyBy(event -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
            @Override
            public void processElement(Event value, KeyedProcessFunction<Boolean, Event, String>.Context ctx, Collector<String> out) throws Exception {
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                out.collect("dataComingTime: " + new Time(currentProcessingTime));
                //注册一个定时器
                ctx.timerService().registerEventTimeTimer(currentProcessingTime + 10 * 1000L);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect("定时器触发时间：" + new Timestamp(timestamp));
            }
        }).print();
        env.execute();
    }
}
