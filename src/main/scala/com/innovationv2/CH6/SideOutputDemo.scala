package com.innovationv2.CH6

import com.innovationv2.utils.Event
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

import java.time.Duration

class SideOutputDemo {
  @Test
  def testSideOutput(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val outputTag = new OutputTag[Event]("Later")
    val stream = env
      .addSource(new SourceFunction[Event] {
        override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
          ctx.collect(Event(0, "a", "./a", 0))
          ctx.collect(Event(1, "a", "./b", 5000))
          ctx.collect(Event(2, "a", "./c", 12000))
          ctx.collect(Event(3, "a", "./d", 15000))
          Thread.sleep(3000)
          ctx.collect(Event(4, "a", "./e", 6000))
          ctx.collect(Event(4, "a", "./e", 7000))
        }

        override def cancel(): Unit = ???
      })
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
      .allowedLateness(Time.seconds(0))
      .sideOutputLateData(outputTag)
      .process(new WindowResult)

    stream.print()
    stream.getSideOutput(outputTag).print("SideOutput")
    env.execute()
  }
}
