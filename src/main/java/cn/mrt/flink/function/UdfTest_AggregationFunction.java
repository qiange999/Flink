package cn.mrt.flink.function;

import cn.mrt.flink.pojo.Event;
import cn.mrt.flink.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;

/*

理解自定义聚合函数
1.首先，需要创建累加器accumulattor，用来存储中间结果

 */
public class UdfTest_AggregationFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(stream, $("user"), $("url"), $("timestamp"), $("rt").rowtime());
        tableEnv.createTemporaryView("eventTable", table);
        //注册系统函数（自定义函数）
        tableEnv.createTemporarySystemFunction("Avg", Avg.class);


        env.execute();
    }

    //单独定义一个累加器
    public static class Accumulator {
        public long sum = 0;
        public int count = 0;
    }

    public static class Avg extends AggregateFunction<Long, Accumulator>{

        @Override
        public Long getValue(Accumulator accumulator) {
            return null;
        }


        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }
        //必须要手动实现的accumulate 类似eval
        public void accumulate(Accumulator acc,Long iValue){
            acc.sum +=iValue;
            acc.count +=1;
        }

    }


}
