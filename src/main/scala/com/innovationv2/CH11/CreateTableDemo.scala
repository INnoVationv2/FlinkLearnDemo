package com.innovationv2.CH11

import com.innovationv2.CH11.FlinkSqlUtils.sendEventToKafka
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Schema, TableDescriptor}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.junit.Test

class CreateTableDemo {
  @Test
  def testCreateTableBySql(): Unit = {
    sendEventToKafka()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val sql =
      """|CREATE TABLE event_kafka(
         |id INT,
         |`user` STRING,
         |url STRING,
         |ts_int BIGINT
         |) WITH (
         |'connector' = 'kafka',
         |'topic' = 'events',
         |'properties.bootstrap.servers' = 'localhost:9092',
         |'properties.group.id' = 'CreateTableDemoConsumer',
         |'scan.startup.mode' = 'latest-offset',
         |'format' = 'avro'
         |)
         |""".stripMargin
    tableEnv.executeSql(sql)

    tableEnv
      .toDataStream(tableEnv.sqlQuery("SELECT * FROM event_kafka"))
      .print(" Result: ")
    env.execute()
  }

  @Test
  def testCreateTableByTableAPI(): Unit = {
    sendEventToKafka()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val tableSchema = Schema.newBuilder()
      .column("id", DataTypes.INT)
      .column("user", DataTypes.STRING)
      .column("url", DataTypes.STRING)
      .column("ts_int", DataTypes.BIGINT)
      .build()
    val tableDescriptor = TableDescriptor
      .forConnector("kafka")
      .schema(tableSchema)
      .format("avro")
      .option("topic", "events")
      .option("properties.bootstrap.servers", "localhost:9092")
      .option("properties.group.id", "CreateTableDemoConsumer")
      .option("scan.startup.mode", "latest-offset")
      .build()
    tableEnv.createTable("event_kafka", tableDescriptor)

    tableEnv
      .toDataStream(tableEnv.sqlQuery("SELECT * FROM event_kafka"))
      .print("Result: ")
    env.execute()
  }
}
