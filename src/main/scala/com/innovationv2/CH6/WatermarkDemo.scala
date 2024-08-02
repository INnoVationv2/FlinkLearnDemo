package com.innovationv2.CH6

import com.innovationv2.utils.{BasicEventSource, Event}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration
import java.util.Calendar


class WatermarkDemo {
  /*
    实验目的: 观察逻辑时钟
    实验思路：
      每3s产生一个Event
      每个Event中的时间戳只比上一个Event中的时间戳增长1s
      观察触发计算时间
  * */
  @Test
  def testWatermark(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env
      .addSource(new CustomEventSource(gap = 3000))
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
      .process(new UrlCountAgg)
      .print()

    env.execute()
  }
}

class CustomEventSource(cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends BasicEventSource(cnt: Int, gap: Long, print: Boolean) {
  override def generateEventElement(): Event = {
    val username = users(random.nextInt(users.length))
    val url = urls(0)
    val event = Event(id, username, url, curTs)
    curTs = curTs + 1000
    id += 1
    event
  }
}

class UrlCountAgg extends ProcessWindowFunction[Event, String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
    out.collect(s"""$key, ${elements.size}, ${context.window.getStart}~${context.window.getEnd}, watermark:${context.currentWatermark}""")
  }
}