package com.innovationv2.CH6

import com.innovationv2.utils.{Event, EventSourceWithTimeStamp}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.junit.Test

import java.sql.Timestamp
import java.time.Duration

class UrlViewCountDemo {
  @Test
  def testUrlViewCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
      .addSource(new EventSourceWithTimeStamp(print = true))
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
      .aggregate(new UrlViewCountAgg, new UrlViewCountResult)
      .print()
    env.execute()
  }
}

class UrlViewCountAgg extends AggregateFunction[Event, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: Event, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = ???
}

case class UrlViewCount(url: String, count: Long, windowStart: Long, windowEnd: Long)

//object UrlViewCount {
//  def apply(url: String, count: Long, windowStart: Long, windowEnd: Long): UrlViewCount = {
//    new UrlViewCount(url, count, new Timestamp(windowStart), new Timestamp(windowEnd))
//  }
//}

class UrlViewCountResult extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(
      key,
      elements.iterator.next(),
      context.window.getStart,
      context.window.getEnd
    ))
  }
}
