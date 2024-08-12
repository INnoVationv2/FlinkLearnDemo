package com.innovationv2.CH11

import com.innovationv2.utils.BasicEventSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.junit.{Before, Test}

// 开窗函数
class OverAggDemo {
  private var env: StreamExecutionEnvironment = _
  private var tableEnv: StreamTableEnvironment = _

  @Before
  def init(): Unit = {
    this.env = StreamExecutionEnvironment.getExecutionEnvironment
    this.tableEnv = StreamTableEnvironment.create(env)
    val stream = env.addSource(new BasicEventSource())
      .assignAscendingTimestamps(_.timestamp)
    val eventTable = tableEnv.fromDataStream(stream, $("id"), $("user"), $("url"), $("timestamp").rowtime().as("ts"))
    tableEnv.createTemporaryView("EventTable", eventTable)
  }

  // 统计从当前行开始往前1s的ID和
  @Test
  def testOverAggByTime(): Unit = {
    val sql =
      """SELECT id, user,
        |SUM(id) OVER (
        | ORDER BY ts
        | RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW
        |) AS sum_id
        |FROM EventTable
        |""".stripMargin
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print("testOverAggByTime: ")
    env.execute()
  }

  // 统计从当前行开始往前1行的ID和
  @Test
  def testOverAggByCnt(): Unit = {
    val sql =
      """SELECT id, user,
        |SUM(id) OVER (
        | ORDER BY ts
        | ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        |) AS sum_id
        |FROM EventTable
        |""".stripMargin
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print("testOverAggByTime: ")
    env.execute()
  }
}
