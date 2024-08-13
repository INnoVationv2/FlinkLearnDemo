package com.innovationv2.CH12

import com.innovationv2.utils.Event
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration
import java.util

case class OrderEvent(userId: String, orderId: String, eventType: String, timestamp: Long)

class TimeOutEventDemo {
  @Test
  def testPatternProcessFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val orderStream =
      env.fromElements(
          OrderEvent("user_1", "order_1", "create", 1000L),
          OrderEvent("user_2", "order_2", "create", 2000L),
          OrderEvent("user_1", "order_1", "modify", 20 * 1000L),
          OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
          OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
          OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L),
        ).assignAscendingTimestamps(_.timestamp)
        .keyBy(_.orderId)

    val pattern = Pattern
      .begin[OrderEvent]("create")
      .where(_.eventType == "create")
      .followedBy("pay")
      .where(_.eventType == "pay")
      .within(Duration.ofMinutes(15))

    val payedStream = CEP.pattern(orderStream, pattern)
      .process(new MyPatternProcessFunction)
    payedStream.print("Payed")
    payedStream.getSideOutput(new OutputTag[String]("timeout")).print("timeout")

    env.execute()
  }
}

class MyPatternProcessFunction extends PatternProcessFunction[OrderEvent, String] with TimedOutPartialMatchHandler[OrderEvent] {
  override def processMatch(matches: util.Map[String, util.List[OrderEvent]], ctx: PatternProcessFunction.Context, out: Collector[String]): Unit = {
    val order = matches.get("pay").get(0)
    out.collect(s"订单: ${order.orderId} 已支付")
  }

  override def processTimedOutMatch(matches: util.Map[String, util.List[OrderEvent]], ctx: PatternProcessFunction.Context): Unit = {
    val order = matches.get("create").get(0)
    val outputTag = new OutputTag[String]("timeout")
    ctx.output(outputTag,
      s"订单: ${order.orderId} 已超时, " +
        s"用户为${order.userId}, " +
        s"订单创建时间:${order.timestamp}")
  }
}