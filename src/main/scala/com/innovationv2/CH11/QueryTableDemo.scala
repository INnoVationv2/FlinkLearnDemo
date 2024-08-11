package com.innovationv2.CH11

import com.innovationv2.CH11.FlinkSqlUtils.{getEventKafkaDescriptor, sendEventToKafka}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.junit.Test

class QueryTableDemo {
  @Test
  def testQueryTableBySQL(): Unit = {
    sendEventToKafka()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val tableDescriptor = getEventKafkaDescriptor(groupId = "QueryTable")
    tableEnv.createTable("event_kafka", tableDescriptor)

    val table = tableEnv.sqlQuery("SELECT * FROM event_kafka WHERE user = 'Alice'")
    tableEnv.toDataStream(table).print("testQueryTableBySQL: ")
    env.execute()
  }

  @Test
  def testQueryTableByTableAPI(): Unit = {
    sendEventToKafka()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val tableDescriptor = getEventKafkaDescriptor(groupId = "QueryTable")
    tableEnv.createTable("event_kafka", tableDescriptor)

    val table = tableEnv.from("event_kafka")
    val queryResult = table
      .where($("user").isEqual("Alice"))
      .select($("*"))
    tableEnv.toDataStream(queryResult).print("testQueryTableByTableAPI: ")
    env.execute()
  }
}
