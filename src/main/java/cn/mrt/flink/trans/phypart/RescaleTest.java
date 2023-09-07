package cn.mrt.flink.trans.phypart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/*
 * @author  cqh
 * @version 1.0
 */
public class RescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new CustomSource()).setParallelism(2).rescale().print().setParallelism(4);


        env.execute();
    }

    public static class CustomSource extends RichParallelSourceFunction{

        @Override
        public void run(SourceContext sourceContext) throws Exception {
            for (int i = 0; i < 8; i++) {
                if (i == getRuntimeContext().getIndexOfThisSubtask()){
                sourceContext.collect(i);    }
            }
        }

        @Override
        public void cancel() {

        }
    }
}

