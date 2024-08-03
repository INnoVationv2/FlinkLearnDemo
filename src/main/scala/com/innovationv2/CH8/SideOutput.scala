package com.innovationv2.CH8

import com.innovationv2.utils.{BasicEventSource, Event}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

class SideOutput {
  @Test
  def testSideOutput(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val aliceStream = new OutputTag[Event]("AliceStream")
    val bobStream = new OutputTag[Event]("Bob")

    val stream = env
      .addSource(new BasicEventSource(print = false))
      .assignAscendingTimestamps(_.timestamp)
      .process(new ProcessFunction[Event, Event] {
        override def processElement(value: Event, ctx: ProcessFunction[Event, Event]#Context, out: Collector[Event]): Unit = {
          if (value.user.equals("Alice"))
            ctx.output(aliceStream, value)
          else if (value.user.equals("Bob"))
            ctx.output(bobStream, value)
          out.collect(value)
        }
      })

    stream.getSideOutput(aliceStream).print("AliceStream: ")
    stream.getSideOutput(bobStream).print("BobStream: ")
    env.execute()
  }
}
