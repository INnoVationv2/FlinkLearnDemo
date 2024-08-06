package com.innovationv2.CH9.KeyedState

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ReducingStateDescriptor}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

class AggregatingStateDemo {
  @Test
  def testAggregatingState(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements(
        ("Alice", 90), ("Alice", 60),
        ("Bob", 80), ("Bob", 70),
        ("Cary", 20)
      ).keyBy(_._1)
      .process(new ProcessFunction[(String, Int), String] {
        lazy val aggState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[(String, Int), (Int, Int), Double]("Agg-state",
          new AggregateFunction[(String, Int), (Int, Int), Double] {
            override def createAccumulator(): (Int, Int) = (0, 0)

            override def add(value: (String, Int), accumulator: (Int, Int)): (Int, Int) = (accumulator._1 + 1, accumulator._2 + value._2)

            override def getResult(accumulator: (Int, Int)): Double = accumulator._2.toDouble / accumulator._1

            override def merge(a: (Int, Int), b: (Int, Int)): (Int, Int) = ???
          }, classOf[(Int, Int)])
        )

        override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), String]#Context, out: Collector[String]): Unit = {
          aggState.add(value)
          out.collect(s"${value._1}: ${aggState.get()}")
        }
      }).print()

    env.execute()
  }
}
