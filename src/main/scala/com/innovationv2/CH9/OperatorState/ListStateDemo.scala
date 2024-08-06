package com.innovationv2.CH9.OperatorState

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.junit.Test

import scala.collection.mutable.ListBuffer

class ListStateDemo {
  @Test
  def testListState(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env
      .fromElements(
        ("Alice", 100), ("Bob", 150),
        ("Cary", 200), ("Deepesh", 250),
        ("Elbow", 300), ("Fury", 100)
      ).addSink(new UserSink(6))

    env.execute()
  }
}

class UserSink(threshold: Int) extends SinkFunction[(String, Int)] with CheckpointedFunction {
  private var checkpointState: ListState[(String, Int)] = _
  private val buffer = ListBuffer[(String, Int)]()

  override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
    buffer.append(value)

    if (buffer.size < threshold)
      return

    println("==============UserSink=================")
    buffer.foreach(println)
    println("==========UserSink Complete=============")
    buffer.clear()
    checkpointState.clear()
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // 创建快照,将buffer保存下来
    checkpointState.clear()
    buffer.foreach(checkpointState.add)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val listStateDescriptor = new ListStateDescriptor[(String, Int)]("Buffered-elements", classOf[(String, Int)])
    //创建ListState
    checkpointState = context.getOperatorStateStore.getListState(listStateDescriptor)
    // 如果是Restored,还需要恢复状态
    if (context.isRestored)
      checkpointState.get().forEach(element => buffer += element)
  }
}
