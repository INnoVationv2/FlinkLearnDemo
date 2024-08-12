package com.innovationv2.CH11

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint, InputGroup}
import org.apache.flink.table.functions._
import org.apache.flink.types.Row
import org.junit.Test

import java.lang

case class STR(str: String)

case class Student(name: String, score: Long, weight: Int)

class UDFDemo extends BasicTestInterface {
  addSourceFlag = false

  @Test
  def testScalarFunction(): Unit = {
    tableEnv.createTemporarySystemFunction("HashFunc", classOf[HashFunction])
    val sql = "SELECT user, HashFunc(user) FROM EventTable"
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print()
    env.execute()
  }

  @Test
  def testTableFunction(): Unit = {
    val stream = env.fromElements(
      STR("Hello World"),
      STR("My name is xxx")
    )
    tableEnv.createTemporaryView("MyTable", stream)

    tableEnv.createTemporarySystemFunction("SplitFunc", classOf[SplitFunction])
    val sql =
      """SELECT
        | str, word, length
        |FROM
        | MyTable, LATERAL TABLE(SplitFunc(str))
        |""".stripMargin
    tableEnv.toDataStream(tableEnv.sqlQuery(sql)).print()
    env.execute()
  }


  @Test
  def testAggFunction(): Unit = {
    val stream = env.fromElements(
      Student("Alice", 100, 4),
      Student("Alice", 80, 6),
      Student("Bob", 85, 4)
    )
    tableEnv.createTemporaryView("UserScore", stream)

    tableEnv.createTemporarySystemFunction("WeightAvgFunc", classOf[WeightAvg])
    val sql =
      """SELECT
        | name, WeightAvgFunc(score, weight)
        |FROM
        | UserScore group by name
        |""".stripMargin
    tableEnv.toChangelogStream(tableEnv.sqlQuery(sql)).print()
    env.execute()
  }
}

//用DataTypeHint(inputGroup=InputGroup.ANY)对输入参数的类型做了标注，表示eval的参数可以是任意类型
class HashFunction extends ScalarFunction {
  def eval(@DataTypeHint(inputGroup = InputGroup.ANY) o: AnyRef): Int = {
    o.hashCode()
  }
}

@FunctionHint(output = new DataTypeHint("Row<word String, length INT>"))
class SplitFunction extends TableFunction[Row] {
  def eval(str: String): Unit = {
    str.split(" ").foreach(s => collect(Row.of(s, Int.box(s.length))))
  }
}

case class WrightAvgAccumulator(var sum: Long = 0L, var count: Int = 0)

class WeightAvg extends AggregateFunction[java.lang.Long, WrightAvgAccumulator] {
  override def createAccumulator(): WrightAvgAccumulator = {
    WrightAvgAccumulator()
  }

  override def getValue(acc: WrightAvgAccumulator): lang.Long = {
    if (acc.count == 0)
      null
    else
      acc.sum / acc.count
  }

  def accumulate(acc: WrightAvgAccumulator, iValue: java.lang.Long, iWeight: Int): Unit = {
    acc.sum += iValue * iWeight
    acc.count += iWeight
  }
}