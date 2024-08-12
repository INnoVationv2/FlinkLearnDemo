package com.innovationv2.CH11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.junit.{Before, Test}

class JoinDemo {
  var env: StreamExecutionEnvironment = _
  var tableEnv: StreamTableEnvironment = _

  @Before
  def init(): Unit = {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    tableEnv = StreamTableEnvironment.create(env)

    val nameStream = env.fromElements(
      (0, "Alice"),
      (1, "Bob"),
      (2, "Cary"),
      (3, "Dave")
    )
    tableEnv.createTemporaryView("student", nameStream)

    val soreStream = env.fromElements(
      (0, "Chinese", 96),
      (0, "Math", 80),
      (1, "Chinese", 88),
      (2, "English", 70)
    )
    tableEnv.createTemporaryView("score", soreStream)
  }

  // student._1: 下标从1开始
  @Test
  def testInnerJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM student
        |INNER JOIN score
        |ON student._1 = score._1
        |""".stripMargin
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print()
    env.execute()
  }

  @Test
  def testLeftJoin(): Unit = {
    val sql =
      """
        |SELECT student.*, score.*
        |FROM score
        |LEFT JOIN student
        |ON student._1 = score._1
        |""".stripMargin
    tableEnv.toChangelogStream(tableEnv.sqlQuery(sql)).print()
    env.execute()
  }

  @Test
  def testRightJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM student
        |RIGHT JOIN score
        |ON student._1 = score._1
        |""".stripMargin
    tableEnv.toChangelogStream(tableEnv.sqlQuery(sql)).print()
    env.execute()
  }

  @Test
  def testFullOuterJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM student
        |FULL OUTER JOIN score
        |ON student._1 = score._1
        |""".stripMargin
    tableEnv.toChangelogStream(tableEnv.sqlQuery(sql)).print()
    env.execute()
  }

  @Test
  def testIntervalJoin(): Unit = {
    val userStream = env.fromElements(
      (0, 50000L), (1, 100000L)
    ).assignAscendingTimestamps(_._2)
    tableEnv.createTemporaryView("userTable", userStream)

    val pvStream = env.fromElements(
      ("./cart", 2000L),
      ("./prod?id=100", 10000L),
      ("./prod?id=1", 20000L),
      ("./prod?id=1", 50000L),
      ("./prod?id=4", 80000L),
      ("./prod?id=2", 110000L),
      ("./prod?id=300", 120000L),
      ("/home", 150000L),
    ).assignAscendingTimestamps(_._2)
    tableEnv.createTemporaryView("pvTable", pvStream)

    val sql =
      """
        |SELECT u._2, p.*
        |FROM userTable u, pvTable p
        |WHERE p._2 BETWEEN u._2 - 30000 AND u._2 + 30000
        |""".stripMargin
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print()
    env.execute()
  }
}
