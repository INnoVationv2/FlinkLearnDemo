package com.innovationv2.CH9

import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration

class TTLDemo {
  @Test
  def testTTL(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val ttlConfig = StateTtlConfig
      //设置过期时长
      .newBuilder(Duration.ofSeconds(3))
      // 设置更新策略
      .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
      // 设置状态可见性
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()

    env.addSource(new UserScoreSource(gap = 5000))
      .keyBy(_._1)
      .process(new ProcessFunction[(String, Long), (String, Long)] {
        private val cntStateDescriptor = new ValueStateDescriptor[Int]("totalVisitors", classOf[Int])
        cntStateDescriptor.enableTimeToLive(ttlConfig)
        lazy val cntState: ValueState[Int] = getRuntimeContext.getState(cntStateDescriptor)

        override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
          cntState.update(cntState.value() + 1)
          println(s"$value, ${cntState.value()}")
        }
      }).print()

    env.execute()
  }
}

class UserScoreSource(gap: Long) extends SourceFunction[(String, Long)] {
  override def run(ctx: SourceFunction.SourceContext[(String, Long)]): Unit = {
    ctx.collect("Alice", 1000L)
    Thread.sleep(1000)

    ctx.collect("Alice", 5000L)
    Thread.sleep(5000L)

    ctx.collect("Alice", 0L)
  }

  override def cancel(): Unit = ???
}
