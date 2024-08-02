package com.innovationv2.CH6

import com.innovationv2.utils.{BasicEventSource, Event}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.junit.Test

import java.lang
import java.time.Duration

class TriggerDemo {
  @Test
  def testTrigger(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env
      .addSource(new TriggerDemoEventSource(gap = 3000))
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Duration.ofSeconds(20)))
      .trigger(new MyTrigger)
      .process(new WindowResult)
      .print()

    env.execute()
  }
}

class TriggerDemoEventSource(cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends BasicEventSource(cnt: Int, gap: Long, print: Boolean) {
  override val sourceName = "TriggerDemoEventSource"
  override val urls: Array[String] = Array("./home")
}

class MyTrigger extends Trigger[Event, TimeWindow] {
  override def onElement(element: Event, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val isFirstEvent = ctx.getPartitionedState(
      new ValueStateDescriptor[Boolean]("first_event", classOf[Boolean])
    )
    if (!isFirstEvent.value()) {
      println(s"${window.getStart}~${window.getEnd}:")
      for (i <- window.getStart to window.getEnd by 5000L) {
        println(s"  Register $i")
        ctx.registerEventTimeTimer(i)
      }
      isFirstEvent.update(true)
    }
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println(s"CurrentTime:$time, Watermark:${ctx.getCurrentWatermark} Called onEventTime")
    TriggerResult.FIRE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.getPartitionedState(
      new ValueStateDescriptor[Boolean]("first_event", classOf[Boolean])
    ).clear()
  }
}

class WindowResult extends ProcessWindowFunction[Event, UrlViewCount, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[UrlViewCount]): Unit = {
    out.collect(
      UrlViewCount(
        key, elements.size,
        context.window.getStart,
        context.window.getEnd
      )
    )
  }
}
