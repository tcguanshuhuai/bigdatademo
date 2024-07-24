package com.sguan.flinsqldemo.commondemo;

import com.sguan.flinsqldemo.source.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class SimpleDemo1 {
  public static void main(String[] args) throws Exception {
    //
      // 1.获取环境
      //1.1获取流环境
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(2);
      // 1.2. 通过流环境获取表环境
      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

      // 1. 读取数据源
      SingleOutputStreamOperator<Event> eventStream = env
              .fromElements(
                      new Event("Alice", "./home", 1000L),
                      new Event("Bob", "./cart", 1000L),
                      new Event("Alice", "./prod?id=1", 5 * 1000L),
                      new Event("Cary", "./home", 60 * 1000L),
                      new Event("Bob", "./prod?id=3", 90 * 1000L),
                      new Event("Alice", "./prod?id=7", 105 * 1000L)
              );

      // 3. 将数据流转换成Table对象
      /**
       * 重要，table 对象 相当与create temporary view的view
       * 但是这个view是不会存储在任何catalog里的，可以认为是
       * 游离在catalog外的view
       */
      Table eventTable = tableEnv.fromDataStream(eventStream);



      // 3. 用DSL
      /**
       * 通过table 对象可以用DSL
       */
      Table resultTable2 = eventTable.select($("user"), $("url"))
              .where($("user").isEqual("Alice"));


      // 4. 用SQL
      /**
       *table对象既然是view，当然可以用sql，只不过sql必须这样
       * "select url, user from " + eventTable（table 对象引用）
       * 而非正常的
       * select url, user from tablename，原因是
       * 1.table对象并没有tablename
       * 2.table对象不注册进任意catalog，即使有tablename也无法找到
       */
      Table resultTable1 = tableEnv.sqlQuery("select url, user from " + eventTable);


      // 6. 将table 对象转换成数据流，打印输出
      tableEnv.toDataStream(resultTable1).print("result1");
      tableEnv.toDataStream(resultTable2).print("result2");
      

      // 执行程序
      env.execute();
  }
}
