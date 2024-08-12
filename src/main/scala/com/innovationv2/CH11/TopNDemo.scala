package com.innovationv2.CH11

import org.junit.Test

class TopNDemo extends BasicTestInterface {
  @Test
  def testTopN1(): Unit = {
    val sql =
      """
        |SELECT id, user, url, ts, row_num
        |FROM (
        | SELECT *,
        |   ROW_NUMBER() OVER(
        |     ORDER BY CHAR_LENGTH(url) DESC
        |   ) AS row_num
        | FROM EventTable
        |)
        |WHERE row_num <= 2
        |""".stripMargin
    // Top N聚合是一个更新查询。新数据到来后，可能会改变之前数据的排名，
    // 所以会有更新(UPDATE)操作。因此, 执行上面的SQL得到结果表，
    // 需要调用toChangelogStream()才能转换成流打印输出。
    tableEnv.toChangelogStream(tableEnv.sqlQuery(sql)).print()
    env.execute()
  }

  @Test
  def testTopN2(): Unit = {
    val subSql =
      """
        |SELECT
        | window_start, window_end, user, COUNT(url) as cnt
        |FROM
        | TABLE(TUMBLE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '10' SECOND))
        |GROUP BY window_start, window_end, user
        |""".stripMargin
    val topNSql =
      s"""
        |SELECT user, cnt, window_start, window_end
        |FROM (
        | SELECT *,
        |   ROW_NUMBER() OVER (
        |     PARTITION BY window_start, window_end
        |     ORDER BY cnt DESC
        |   ) AS row_num
        | FROM ($subSql)
        |)
        |WHERE row_num <= 2
        |""".stripMargin
    tableEnv.toDataStream(tableEnv.sqlQuery(topNSql)).print()
    env.execute()
  }
}
