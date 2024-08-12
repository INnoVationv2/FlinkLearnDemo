package com.innovationv2.CH11

import org.junit.Test

// 开窗函数
class OverAggDemo extends BasicTestInterface {
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

  // 开一个窗口，其中包含本条和前一条数据，求出url较长的长度
  @Test
  def testOverWindow(): Unit = {
    val sql =
      """SELECT id, user,
        | SUM(id) OVER w AS sum_id,
        | MAX(CHAR_LENGTH(url)) OVER w as max_url
        |FROM EventTable
        |WINDOW w AS (
        | ORDER BY ts
        | ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        |)
        |""".stripMargin
    println(sql)
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print("testOverAggByTime: ")
    env.execute()
  }
}
