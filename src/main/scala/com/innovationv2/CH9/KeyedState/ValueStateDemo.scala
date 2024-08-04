package com.innovationv2.CH9.KeyedState

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

class ValueStateDemo {
  @Test
  def testValueState(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements(
        ("Alice", 90),
        ("Bob", 80),
        ("Alice", 70),
        ("Bob", 60),
      ).keyBy(_._1)
      .process(new ProcessFunction[(String, Int), (String, Int)] {
        val totalScore: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("totalScore", classOf[Int]))

        override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          totalScore.update(totalScore.value() + value._2)
          out.collect((value._1, totalScore.value()))
        }
      }).print()

    env.execute()
  }
}
