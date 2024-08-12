package com.innovationv2.CH11

import org.junit.Test

class WindowDemo extends BasicTestInterface {
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
