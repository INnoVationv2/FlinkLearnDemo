package com.innovationv2.CH8

import com.innovationv2.utils.Event
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration

class IntervalJoinDemo {
  @Test
  def testIntervalJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val orderStream = env.fromElements(
      ("Alice", "order-2", 50000L),
      ("Bob", "order-3", 100000L),
    ).assignAscendingTimestamps(_._3)

    val pvStream = env.fromElements(
      Event(0, "Mary", "./cart", 2000L),
      Event(1, "Alice", "./prod?id=100", 10000L),
      Event(2, "Alice", "./prod?id=1", 20000L),
      Event(4, "Bob", "./prod?id=1", 50000L),
      Event(3, "Alice", "./prod?id=4", 80000L),
      Event(5, "Bob", "./prod?id=2", 110000L),
      Event(6, "Alice", "./prod?id=300", 120000L),
      Event(7, "Jane", "/home", 150000L),
    ).assignAscendingTimestamps(_.timestamp)

    orderStream
      .keyBy(_._1)
      .intervalJoin(pvStream.keyBy(_.user))
      .between(Time.seconds(-30), Time.seconds(30))
      .process(new ProcessJoinFunction[(String, String, Long), Event, String] {
        override def processElement(left: (String, String, Long), right: Event, ctx: ProcessJoinFunction[(String, String, Long), Event, String]#Context, out: Collector[String]): Unit = {
          out.collect(s"$left(${left._3 - Duration.ofSeconds(30).toMillis}<=${left._3}<=${left._3 + Duration.ofSeconds(30).toMillis}) => $right")
        }
      })
      .print()

    env.execute()
  }
}
