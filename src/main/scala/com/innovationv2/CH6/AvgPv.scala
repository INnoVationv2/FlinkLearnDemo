package com.innovationv2.CH6

import com.innovationv2.utils.{BasicEventSource, Event, EventSourceWithTimeStamp}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.junit.Test

import java.time.Duration

class AvgPv {
  @Test
  def StatisticsAvgPv(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
      .addSource(new BasicEventSource(print = true))
      .assignAscendingTimestamps(_.timestamp)
      //.addSource(new EventSourceWithTimeStamp(print = true))
      .keyBy(_ => "key")
      .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
      .aggregate(new AggregateAvgPv)
      .print()
    env.execute()
  }
}

/*
  PV: 网站被访问次数
  UV：有多少个不同用户
  AvgPV: 人均重复访问量
*/
class AggregateAvgPv extends AggregateFunction[Event, (Set[String], Double), Double] {
  override def createAccumulator(): (Set[String], Double) = (Set[String](), 0L)

  // Set[String]统计用户个数，Double统计访问次数
  override def add(value: Event, accumulator: (Set[String], Double)): (Set[String], Double) = {
    val tmp = (accumulator._1 + value.user, accumulator._2 + 1L)
    printDetail(tmp)
    tmp
  }

  override def getResult(accumulator: (Set[String], Double)): Double = {
    accumulator._2 / accumulator._1.size
  }

  override def merge(a: (Set[String], Double), b: (Set[String], Double)): (Set[String], Double) = ???

  private def printDetail(accumulator: (Set[String], Double)): Unit = {
    println(s"""PV:${accumulator._2}, UV:${accumulator._1.size}""")
  }
}
