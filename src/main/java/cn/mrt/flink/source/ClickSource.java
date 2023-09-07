package cn.mrt.flink.source;

import cn.mrt.flink.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.*;

/*
 * @author  cqh
 * @version 1.0
 */
//<>用来表示产生什么类型的数据的
public class ClickSource implements SourceFunction<Event> {
    //用来控制数据源是否继续发送的参数

    private boolean running = true;
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        //
        Random random = new Random();
        //生成数据的随机源
        List<String> users = Arrays.asList("Mary", "Alice", "Bob", "Cary");
        List<String> urls = Arrays.asList("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2");

        //循环发送，随机生成
        while (running){
            String user = users.get(random.nextInt(users.size()));
            String url = urls.get(random.nextInt(urls.size()));
            long timestamp = Calendar.getInstance().getTimeInMillis();

            //创建Event
            Event event = new Event(user, url, timestamp);
            //发送数据
            sourceContext.collect(event);
            //
            Thread.sleep(1000);
        }
    }
    //用来取消数据的发送
    @Override
    public void cancel() {
        running = false;
    }
}
