package com.innovationv2.CH7.TopN

import com.innovationv2.utils.{BasicEventSource, Event}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration
import scala.collection.mutable

class ProcessAllWindowFunctionTopN {
  @Test
  def testTopN(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env
      .addSource(new BasicEventSource(gap = 100))
      .assignAscendingTimestamps(_.timestamp)
      .windowAll(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
      .process(new TopNProcessWindowFunction)
      .print()

    env.execute()
  }
}

class TopNProcessWindowFunction extends ProcessAllWindowFunction[Event, (String, Int), TimeWindow] {
  override def process(context: Context, elements: Iterable[Event], out: Collector[(String, Int)]): Unit = {
    val map = mutable.HashMap[String, Int]()
    val iterator = elements.iterator
    while (iterator.hasNext) {
      val element = iterator.next()
      if (!map.contains(element.url))
        map(element.url) = 0
      map(element.url) += 1
    }
    if (map.isEmpty) return
    val array = map.toArray.sortBy(_._2)(Ordering[Int].reverse)

    out.collect(array(0))
    if (array.length > 1)
      out.collect(array(1))
  }
}
