package com.innovationv2.CH12

import com.innovationv2.utils.LoginEvent
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.junit.Test

import java.util

class Alarm extends CEPBasicTestInterface {
  @Test
  def testAlarm(): Unit = {
    val pattern = Pattern
      .begin[LoginEvent]("first")
      .where(_.eventType.equals("fail"))
      .next("second")
      .where(_.eventType.equals("fail"))
      .next("third")
      .where(_.eventType.equals("fail"))

    val patternStream = CEP.pattern(loginEventStream, pattern)
    patternStream
      .select(new PatternSelectFunction[LoginEvent, String] {
        override def select(matches: util.Map[String, util.List[LoginEvent]]): String = {
          val first = matches.get("first").get(0)
          val second = matches.get("second").get(0)
          val third = matches.get("third").get(0)
          s"Alarm: 连续三次登录失败，登录时间: ${first.timestamp},${second.timestamp},${third.timestamp} "
        }
      })
      .print("Alarm: ")

    env.execute()
  }
}



