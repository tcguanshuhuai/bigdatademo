package com.atguigu.processfuctiontest;

import com.atguigu.source.ClickSource;
import com.atguigu.source.Event;
import com.atguigu.source.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

public class KeyedProcessTopN {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // 从自定义数据源读取数据
    SingleOutputStreamOperator<Event> eventStream =
        env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Event>() {
                          @Override
                          public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                          }
                        }));

    // 需要按照url分组，求出每个url的访问量
    SingleOutputStreamOperator<UrlViewCount> urlCountStream =
        eventStream
            .keyBy(data -> data.url)
                //每30s一个窗口，滑动步长20s
            .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(20)))
                //窗口计算每个url的count
            .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

    // 对结果中同一个窗口的统计数据，进行排序处理
    SingleOutputStreamOperator<String> result =
            //keyby进行分区，这时候没有窗口，相当与所有时间是一个大窗口，所以不管前面的window数据多么晚过来，都能通过keyby拿到
            //每条数据都能触发keyby和process
        urlCountStream.keyBy(data -> data.windowEnd).process(new TopN(2));

    result.print("result");

    env.execute();
  }

  // 自定义增量聚合
  public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long add(Event value, Long accumulator) {
      return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
      return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
      return null;
    }
  }

  // 自定义全窗口函数，只需要包装窗口信息
  public static class UrlViewCountResult
      extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

    @Override
    public void process(
        String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out)
        throws Exception {
      // 结合窗口信息，包装输出内容
      Long start = context.window().getStart();
      Long end = context.window().getEnd();
      out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
    }
  }

  // 自定义处理函数，排序取top n
  public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {

    // 将n作为属性
    private Integer n;
    // 定义一个列表状态
    private ListState<UrlViewCount> urlViewCountListState;

    public TopN(Integer n) {
      this.n = n;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      // 从环境中获取列表状态句柄，这个只是该key( window end )的liststate,不是全局的
      urlViewCountListState =
          getRuntimeContext()
              .getListState(
                  new ListStateDescriptor<UrlViewCount>(
                      "url-view-count-list", Types.POJO(UrlViewCount.class)));
    }

    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out)
        throws Exception {
      // 将count数据添加到列表状态中，保存起来
      urlViewCountListState.add(value);
      // 注册 window end + 1ms后的定时器，等待所有数据到齐开始排序,
      /**
       * 1.虽然每条记录都会注册一个定时器，但是因为定时器的时间(ctx.getCurrentKey() + 1)是相同的，所以相同key只有一个定时器，
       * 时间是上面窗口的end+1
       * 2.肯定会有迟到数据问题
       */
      ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
        throws Exception {
      // 将数据从列表状态变量中取出，放入ArrayList，方便排序
      ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
      for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
        urlViewCountArrayList.add(urlViewCount);
      }
      // 清空状态，释放资源，
      urlViewCountListState.clear();

      // 排序
      urlViewCountArrayList.sort(
          new Comparator<UrlViewCount>() {
            @Override
            public int compare(UrlViewCount o1, UrlViewCount o2) {
              return o2.count.intValue() - o1.count.intValue();
            }
          });

      // 取前两名，构建输出结果
      StringBuilder result = new StringBuilder();
      result.append("========================================\n");
      result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
      for (int i = 0; i < this.n; i++) {
        UrlViewCount UrlViewCount = urlViewCountArrayList.get(i);
        String info =
            "No."
                + (i + 1)
                + " "
                + "url："
                + UrlViewCount.url
                + " "
                + "浏览量："
                + UrlViewCount.count
                + "\n";
        result.append(info);
      }
      result.append("========================================\n");
      out.collect(result.toString());
    }
  }
}
