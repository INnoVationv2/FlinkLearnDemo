package com.innovationv2.CH11

import com.innovationv2.utils.BasicEventSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.junit.Before

class BasicTestInterface(var addSourceFlag: Boolean = true) {
  var env: StreamExecutionEnvironment = _
  var tableEnv: StreamTableEnvironment = _


  @Before
  def initEnv(): Unit = {
    this.env = StreamExecutionEnvironment.getExecutionEnvironment
    this.tableEnv = StreamTableEnvironment.create(env)
    if (addSourceFlag)
      addSource()
  }

  private def addSource(): Unit = {
    val stream = env.addSource(new BasicEventSource())
      .assignAscendingTimestamps(_.timestamp)
    val eventTable = tableEnv.fromDataStream(stream, $("id"), $("user"), $("url"), $("timestamp").rowtime().as("ts"))
    tableEnv.createTemporaryView("EventTable", eventTable)
  }
}
