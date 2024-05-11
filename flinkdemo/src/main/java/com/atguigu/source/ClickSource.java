package com.atguigu.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
// 在指定的数据集中随机选取数据 

        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1",
                "./prod?id=2"};

        // 无限循环生成数据
        while (running) {
           //collect方法就是生成一个message，并发送到下游节点
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 隔1秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }

    @Override
    //这个方法在kill fink job的时候会调用
    public void cancel() {
        running = false;
    }

}