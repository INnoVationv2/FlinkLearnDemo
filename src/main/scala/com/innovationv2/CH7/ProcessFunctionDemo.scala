package com.innovationv2.CH7

import com.innovationv2.utils.{BasicEventSource, Event}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

class ProcessFunctionDemo {
  @Test
  def testProcessFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new BasicEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .process(new ProcessFunction[Event, String] {
        override def processElement(value: Event, ctx: ProcessFunction[Event, String]#Context, out: Collector[String]): Unit = {
          if (value.user.equals("Mary")) {
            out.collect(value.user)
          } else if (value.user.equals("Bob")) {
            out.collect(value.user)
            out.collect(value.user)
          }
          println(ctx.timerService().currentWatermark())
        }
      })
      .print()

    env.execute()
  }
}
