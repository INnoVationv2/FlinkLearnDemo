package com.innovationv2.CH8

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.util.Collector
import org.junit.Test

import java.lang
import java.time.Duration

class CoGroupFunctionDemo {
  @Test
  def testCoGroupFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.fromElements(
      ("a", 1000L),
      ("b", 1000L),
      ("a", 2000L),
      ("b", 2000L)
    ).assignAscendingTimestamps(_._2)

    val stream2 = env.fromElements(
      ("a", 3000L),
      ("b", 3000L),
      ("a", 4000L),
      ("b", 4000L)
    ).assignAscendingTimestamps(_._2)

    stream1
      .coGroup(stream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
      .apply(new CoGroupFunction[(String, Long), (String, Long), String] {
        override def coGroup(first: lang.Iterable[(String, Long)], second: lang.Iterable[(String, Long)], out: Collector[String]): Unit = {
          out.collect(s"$first => $second")
        }
      }).print()

    env.execute()
  }

}
