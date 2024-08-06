package com.innovationv2.CH9.OperatorState

import com.innovationv2.utils.ColoredOutput.{greenPrint, purplePrint, yellowPrint}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

import scala.util.Random


case class Action(name: String, action: String)

case class Pattern(action1: String, action2: String)

class BroadcastStateDemo {
  @Test
  def testBroadcastState(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val actionStream = env.addSource(new ActionSource(1000))
    val patternStream = env.addSource(new PatternSource())

    val bcStateDescriptor = new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern])
    val bcPatternStream = patternStream.broadcast(bcStateDescriptor)

    val matches = actionStream
      .keyBy(_.name)
      .connect(bcPatternStream)
      .process(new PatternMatch)

    matches.print()
    env.execute()
  }
}

class PatternMatch extends KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)] {
  lazy val prevActionState = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastAction", classOf[String]))
  lazy val bcStateDescriptor = new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern])

  override def processElement(action: Action, ctx: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#ReadOnlyContext, out: Collector[(String, Pattern)]): Unit = {
    val bcState = ctx.getBroadcastState(bcStateDescriptor)
    val pattern: Pattern = bcState.get(Unit)
    val prevAction = prevActionState.value()
    if (pattern != null && prevAction != null)
      if (prevAction == pattern.action1 && action.action == pattern.action2)
        greenPrint(s"Matched: ${action.name}, $pattern")
    //        out.collect(action.name, pattern)
    prevActionState.update(action.action)
  }

  override def processBroadcastElement(pattern: Pattern, ctx: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#Context, out: Collector[(String, Pattern)]): Unit = {
    val bcState = ctx.getBroadcastState(bcStateDescriptor)
    bcState.put(Unit, pattern)
  }
}

class ActionSource(gap: Long) extends SourceFunction[Action] {
  private val nameList = Array("Alice", "Bob")
  private val actionList = Array("Buy", "Pay")
  private val random = new Random()

  override def run(ctx: SourceFunction.SourceContext[Action]): Unit = {
    while (true) {
      val name = nameList(random.nextInt(nameList.length))
      val action = actionList(random.nextInt(actionList.length))
      ctx.collect(Action(name, "login"))
      ctx.collect(Action(name, action))
      println(s"    ActionSource: $name, (login, $action)")
      Thread.sleep(gap)
    }
  }

  override def cancel(): Unit = ???
}

class PatternSource extends SourceFunction[Pattern] {
  override def run(ctx: SourceFunction.SourceContext[Pattern]): Unit = {
    ctx.collect(Pattern("login", "Buy"))
    purplePrint("PatternSource: (login, Buy)")

    Thread.sleep(10000)

    ctx.collect(Pattern("login", "Pay"))
    purplePrint("PatternSource: (login, Pay)")
  }

  override def cancel(): Unit = ???
}

