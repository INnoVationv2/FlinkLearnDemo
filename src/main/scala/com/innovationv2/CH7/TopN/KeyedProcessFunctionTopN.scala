package com.innovationv2.CH7.TopN

import com.innovationv2.CH6.UrlViewCount
import com.innovationv2.utils.{BasicEventSource, Event}
import org.apache.flink.api.common.functions.{AggregateFunction, OpenContext}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.junit.Test

import java.sql.Timestamp
import java.time.Duration

class KeyedProcessFunctionTopN {
  @Test
  def testTopN(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val originStream = env
      .addSource(new BasicEventSource(gap = 100))
      .assignAscendingTimestamps(_.timestamp)

    val viewCountStream = originStream
      .keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
      .aggregate(new UrlCountAgg, new UrlViewResultProcess)

    viewCountStream
      .keyBy(_.windowEnd)
      .process(new TopNKeyedProcessWindowFunction(2))
      .print()

    env.execute()
  }
}


class UrlCountAgg extends AggregateFunction[Event, Int, Int] {
  override def createAccumulator(): Int = 0

  override def add(value: Event, accumulator: Int): Int = accumulator + 1

  override def getResult(accumulator: Int): Int = accumulator

  override def merge(a: Int, b: Int): Int = ???
}


class UrlViewResultProcess extends ProcessWindowFunction[Int, UrlViewCount, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Int], out: Collector[UrlViewCount]): Unit = {
    val iterator = elements.iterator
    while (iterator.hasNext) {
      val urlViewCount = UrlViewCount(key, iterator.next(), context.window.getStart, context.window.getEnd)
      out.collect(urlViewCount)
    }
  }
}

class TopNKeyedProcessWindowFunction(n: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  private var urlViewCountListState: ListState[UrlViewCount] = _

  override def open(openContext: OpenContext): Unit = {
    urlViewCountListState = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount]("list-state", classOf[UrlViewCount])
    )
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    import scala.collection.JavaConversions._
    var urlViewCountList = urlViewCountListState.get().toList
    urlViewCountListState.clear()
    urlViewCountList = urlViewCountList.sortBy(-_.count)
    val result = new StringBuilder()
    result.append("\n========================================")
    for (idx <- 0 to n) {
      val urlViewCount = urlViewCountList(idx)
      result
        .append(
          s"""
             |  浏览量No.${idx + 1}
             |  url: ${urlViewCount.url}
             |  浏览量: ${urlViewCount.count}
             |  窗口结束时间: ${new Timestamp(timestamp)}
             |""".stripMargin)
    }
    result.append("========================================\n")
    out.collect(result.toString())
  }

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlViewCountListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }
}