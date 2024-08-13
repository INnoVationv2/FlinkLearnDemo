package com.innovationv2.CH12

import com.innovationv2.utils.{Event, LoginEvent}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.junit.Test

import java.time.Duration
import java.util
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class IndividualPattern extends CEPBasicTestInterface {
  // 测试限制子类型
  @Test
  def testSubtype(): Unit = {
    //
  }

  @Test
  def testCycleMode(): Unit = {
    val pattern = Pattern
      .begin[LoginEvent]("failEvent")
      .where(_.eventType.equals("fail"))
      .times(3).consecutive()

    val patternStream = CEP.pattern(loginEventStream, pattern)
    patternStream
      .select(new PatternSelectFunction[LoginEvent, String] {
        override def select(matches: util.Map[String, util.List[LoginEvent]]): String = {
          // 一个Pattern匹配多次
          val failEvent = matches.get("failEvent")
          val first = failEvent.get(0)
          val second = failEvent.get(1)
          val third = failEvent.get(2)
          s"Alarm: 连续三次登录失败，登录时间: ${first.timestamp},${second.timestamp},${third.timestamp} "
        }
      })
      .print("Alarm: ")

    env.execute()
  }


  // 测试迭代条件
  @Test
  def testIterativeCondition(): Unit = {
    val pattern = Pattern
      .begin[LoginEvent]("first")
      .where(_.eventType.equals("fail"))
      .within(Duration.ofSeconds(10))
      .next("second")
      .where(_.eventType.equals("fail"))
      .next("third")
      .where(_.eventType.equals("fail"))
      .next("forth")
      .where(new IterativeCondition[LoginEvent] {
        override def filter(value: LoginEvent, ctx: IterativeCondition.Context[LoginEvent]): Boolean = {
          // 获取之前匹配的结果
          val first = ctx.getEventsForPattern("first").head
          println(s"first: $first")

          val second = ctx.getEventsForPattern("second").head
          println(s"second: $second")

          val third = ctx.getEventsForPattern("third").head
          println(s"third: $third")
          value.eventType.equals("fail")
        }
      })

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

  @Test
  def testComposeCondition(): Unit = {
    val pattern = Pattern
      .begin[LoginEvent]("first")
      .where(_.timestamp.equals(1000L))
      .or(_.timestamp.equals(3000L))
      .where(_.timestamp.equals(2000L))

    val patternStream = CEP.pattern(loginEventStream, pattern)
    patternStream
      .select(new PatternSelectFunction[LoginEvent, String] {
        override def select(matches: util.Map[String, util.List[LoginEvent]]): String = {
          val first = matches.get("first").get(0)
          first.toString
        }
      })
      .print("testComposeCondition: ")

    env.execute()
  }

  @Test
  def testPatternGroup(): Unit = {
    val stream = env.fromElements(
      Event(0, "a", "a1", 1000L),
      Event(1, "a", "a2", 2000L),
      Event(2, "a", "a3", 3000L),
      Event(3, "b", "b", 4000L),
    ).assignAscendingTimestamps(_.timestamp)

//    val pattern = Pattern.begin[Event]("a").where(_.user.equals("a")).oneOrMore
//      .followedBy("b").where(_.user.equals("b"))
//    utils.matchAndPrint("Origin", stream, pattern)
//
//    val patternNoSkip = Pattern.begin[Event]("a", AfterMatchSkipStrategy.noSkip()).where(_.user.equals("a")).oneOrMore
//      .followedBy("b").where(_.user.equals("b"))
//    utils.matchAndPrint("NoSkip", stream, patternNoSkip)

//    val patternSkipToNext = Pattern.begin[Event]("a", AfterMatchSkipStrategy.skipToNext()).where(_.user.equals("a")).oneOrMore
//      .followedBy("b").where(_.user.equals("b"))
//    utils.matchAndPrint("SkipToNext", stream, patternSkipToNext)

//    val patternSkipPastLastEvent = Pattern.begin[Event]("a", AfterMatchSkipStrategy.skipPastLastEvent()).where(_.user.equals("a")).oneOrMore
//      .followedBy("b").where(_.user.equals("b"))
//    utils.matchAndPrint("PastLastEvent", stream, patternSkipPastLastEvent)

//    val patternSkipToFirst = Pattern.begin[Event]("a", AfterMatchSkipStrategy.skipToFirst("a")).where(_.user.equals("a")).oneOrMore
//      .followedBy("b").where(_.user.equals("b"))
//    utils.matchAndPrint("SkipToFirst", stream, patternSkipToFirst)

    val patternSkipToLast = Pattern.begin[Event]("a", AfterMatchSkipStrategy.skipToLast("a")).where(_.user.equals("a")).oneOrMore
      .followedBy("b").where(_.user.equals("b"))
    utils.matchAndPrint("SkipToLast", stream, patternSkipToLast)

    env.execute()
  }
}

object utils {
  def matchAndPrint(name: String, stream: DataStream[Event], pattern: Pattern[Event, Event]): Unit = {
    CEP.pattern(stream, pattern)
      .select(new PatternSelectFunction[Event, String] {
        override def select(matches: util.Map[String, util.List[Event]]): String = {
          (matches.get("a").map(_.url) ++ matches.get("b").map(_.url)).mkString(",")
        }
      })
      .print()
  }
}



