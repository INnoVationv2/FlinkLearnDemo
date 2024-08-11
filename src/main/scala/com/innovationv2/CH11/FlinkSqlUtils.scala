package com.innovationv2.CH11

import com.innovationv2.utils.BasicEventSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.Schema.Builder
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, Schema, TableDescriptor}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object FlinkSqlUtils {
  def getTimestampEventSchema: Builder = {
    Schema.newBuilder()
      .column("id", DataTypes.INT)
      .column("user", DataTypes.STRING)
      .column("url", DataTypes.STRING)
      .column("ts_int", DataTypes.BIGINT)
  }

  def getEventKafkaDescriptor(tableSchemaBuilder: Builder = getTimestampEventSchema, groupId: String = "TimestampEventProducer"): TableDescriptor = {
    TableDescriptor
      .forConnector("kafka")
      .schema(tableSchemaBuilder.build())
      .format("avro")
      .option("topic", "events")
      .option("properties.bootstrap.servers", "localhost:9092")
      .option("properties.group.id", groupId)
      .option("scan.startup.mode", "latest-offset")
      //.option(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      //.option("properties.auto.offset.reset", "earliest")
      .build()
  }

  def sendEventToKafka(): Unit = {
    Future {
      _sendEventToKafka()
    }
  }

  private def _sendEventToKafka(): Unit = {
    println("Sending Event Data To Kafka")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.createTable("event_kafka", getEventKafkaDescriptor())

    // 将流转换成表
    val eventStream = env.addSource(new BasicEventSource(cnt = 100))
    // 把数据写入Kafka
    tableEnv.fromDataStream(eventStream)
      .executeInsert("event_kafka")
      .print()

    env.execute()
  }

}
