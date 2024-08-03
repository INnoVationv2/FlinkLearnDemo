package com.innovationv2.CH6

import com.innovationv2.utils.{BasicEventSource, Event}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.junit.Test
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

class HandleLateData {
  @Test
  def testLateData(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val lateDataTag = new OutputTag[Event]("Late")

    val stream = env
      .addSource(new BasicEventSource)
      .assignAscendingTimestamps(_.timestamp)
      // 延迟10s
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(
            Duration.ofSeconds(10)))
      .keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Duration.ofSeconds(20)))
      //允许迟到20s
      .allowedLateness(Time.seconds(20))
      //迟到数据输出到测流
      .sideOutputLateData(lateDataTag)
      .process(new WindowResult)

    stream.print()
    stream.getSideOutput(lateDataTag).print("SideOutput")

    env.execute()
  }
}
