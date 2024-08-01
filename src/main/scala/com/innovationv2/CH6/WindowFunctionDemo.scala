package com.innovationv2.CH6

import com.innovationv2.utils.{BasicEventSource, Event, EventSourceWithTimeStamp}
import org.apache.commons.net.ntp.TimeStamp
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.junit.Test
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
 import java.sql.Timestamp

import java.lang
import java.time.Duration

class WindowFunctionDemo {
  @Test
  def testProcessWindowFunctionUv(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env
      .addSource(new BasicEventSource(print = true))
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_ => "key")
      .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
      .process(new UvCountByWindow)
      .print()

    env.execute()
  }
}

class UvCountByWindow extends ProcessWindowFunction[Event, String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
    var userSet = Set[String]()
    elements.foreach(userSet += _.user)
    val start = context.window.getStart
    val end = context.window.getEnd
    out.collect(s"""Window:${new Timestamp(start)}~${new Timestamp(end)}, UV:${userSet.size}"""")
  }
}
