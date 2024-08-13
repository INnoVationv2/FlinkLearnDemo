package com.innovationv2.CH12

import com.innovationv2.utils.LoginEvent
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

import java.util

class ProcessDemo extends CEPBasicTestInterface {
  @Test
  def testPatternSelectFunction(): Unit = {
    val pattern = Pattern
      .begin[LoginEvent]("failEvent")
      .where(_.eventType.equals("fail"))
      .times(2).consecutive()

    val patternStream = CEP.pattern(loginEventStream, pattern)
    patternStream
      .select(new PatternSelectFunction[LoginEvent, String] {
        override def select(matches: util.Map[String, util.List[LoginEvent]]): String = {
          val failEvent = matches.get("failEvent")
          val first = failEvent.get(0)
          val second = failEvent.get(1)
          s"$first ,$second"
        }
      })
      .print()

    env.execute()
  }

  @Test
  def testPatternFlatSelectFunction(): Unit = {
    val pattern = Pattern
      .begin[LoginEvent]("failEvent")
      .where(_.eventType.equals("fail"))
      .times(2).consecutive()

    val patternStream = CEP.pattern(loginEventStream, pattern)
    patternStream
      .flatSelect(new PatternFlatSelectFunction[LoginEvent, String] {
        override def flatSelect(matches: util.Map[String, util.List[LoginEvent]], out: Collector[String]): Unit = {
          val failEvent = matches.get("failEvent")
          val first = failEvent.get(0)
          val second = failEvent.get(1)
          out.collect(first.toString)
          out.collect(second.toString)
        }
      })
      .print()

    env.execute()
  }

  @Test
  def testPatternProcessFunction(): Unit = {
    val pattern = Pattern
      .begin[LoginEvent]("failEvent")
      .where(_.eventType.equals("fail"))
      .times(2).consecutive()

    val patternStream = CEP.pattern(loginEventStream, pattern)
    patternStream
      .process(new PatternProcessFunction[LoginEvent, String] {
        override def processMatch(matched: util.Map[String, util.List[LoginEvent]], ctx: PatternProcessFunction.Context, out: Collector[String]): Unit = {

        }
      })
      .print()

    env.execute()
  }
}
