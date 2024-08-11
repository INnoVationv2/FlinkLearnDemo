package com.innovationv2.CH11

import com.innovationv2.utils.BasicEventSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.junit.{Before, Test}

class WindowDemo {
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

  @Test
  def testTumble(): Unit = {
    val sql =
      """SELECT
        |SUM(id)
        |FROM
        |TABLE(TUMBLE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '5' SECOND))
        |GROUP BY
        |window_start, window_end""".stripMargin
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print("Result: ")
    env.execute()
  }

  @Test
  def testHop(): Unit = {
    val sql =
      """SELECT
        |SUM(id)
        |FROM
        |TABLE(HOP(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' SECOND, INTERVAL '2' SECOND))
        |GROUP BY
        |window_start, window_end""".stripMargin
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print("Result: ")
    env.execute()
  }

  @Test
  def testCumulate(): Unit = {
    val sql =
      """SELECT
        |SUM(id)
        |FROM
        |TABLE(CUMULATE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '60' SECOND))
        |GROUP BY
        |window_start, window_end""".stripMargin
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print("Result: ")
    env.execute()
  }
}
