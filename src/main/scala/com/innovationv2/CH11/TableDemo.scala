package com.innovationv2.CH11

import com.innovationv2.utils.BasicEventSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.junit.Test

class TableDemo {
  @Test
  def testTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceStream = env.addSource(new BasicEventSource)

    val tableEnv = StreamTableEnvironment.create(env)
    val eventTable = tableEnv.fromDataStream(sourceStream)
    tableEnv.createTemporaryView("event_table", eventTable)
    val queryResult = tableEnv.sqlQuery(s"select id, user from event_table")
    tableEnv.toDataStream(queryResult).print()

    env.execute()
  }
}
