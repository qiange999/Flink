package cn.mrt.flink.window;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import java.sql.Timestamp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

public class UvCountByWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> streamWithTimestamp = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                }));


        //将数据全部发往同一个窗口
        ProcessWindowFunction<Event, String, Boolean, TimeWindow> function = new ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {
            @Override
            public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> events, Collector<String> collector) throws Exception {
                //统计UV 这个窗口所有的数据都再elements
                //相当于要把elements的数据全部拿出来，去做去重计算
                HashSet<String> userSet = new HashSet<>();
                for (Event event : events) {
                    userSet.add(event.user);
                }

                long PV = events.spliterator().estimateSize();
                //结合窗口信息，包装输出内容
                long start = context.window().getStart();
                long end = context.window().getEnd();
                collector.collect("时间段: " + new Timestamp(start) + "~" + new Timestamp(end)+ " UV：" + userSet + " PV: " + PV );
            }

        };
        //真正执行的方法
        SingleOutputStreamOperator<String> processStream = streamWithTimestamp.keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(function);
        processStream.print();
        env.execute();
    }
}
