package com.innovationv2.CH9.KeyedState

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

class ReduceStateDemo {
  @Test
  def testValueState(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements(
        ("Alice", 90), ("Alice", 60),
        ("Bob", 80), ("Bob", 70),
        ("Cary", 20)
      ).keyBy(_._1)
      .process(new ProcessFunction[(String, Int), String] {
        lazy val reduceState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Int]("reduce-state",
          new ReduceFunction[Int] {
            override def reduce(value1: Int, value2: Int): Int = value1 + value2
          }, classOf[Int])
        )

        override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), String]#Context, out: Collector[String]): Unit = {
          reduceState.add(value._2)
          out.collect(s"${value._1},${reduceState.get()}")
        }
      }).print()

    env.execute()
  }
}
