package com.innovationv2.CH12

import com.innovationv2.utils.LoginEvent
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.junit.Test

class AutomationDemo extends CEPBasicTestInterface {
  @Test
  def testAutomation(): Unit = {
    loginEventStream.flatMap(new StateMachineMapper).print("Alarm")
    env.execute()
  }
}

class StateMachineMapper extends RichFlatMapFunction[LoginEvent, String] {
  private lazy val currentState = getRuntimeContext.getState(new ValueStateDescriptor[State]("state", classOf[State]))

  override def flatMap(in: LoginEvent, out: Collector[String]): Unit = {
    if (currentState.value() == null)
      currentState.update(Initial)

    val nextState = Util.transition(currentState.value(), in.eventType)
    nextState match {
      case Matched => out.collect(s"${in.userId} 连续3次登录失败")
      case Terminal => currentState.update(Initial)
      case _ => currentState.update(nextState)
    }
  }
}

sealed trait State
case object Initial extends State
case object Terminal extends State
case object Matched extends State
case object S1 extends State
case object S2 extends State

object Util {
  def transition(state: State, event: String): State = {
    (state, event) match {
      case (Initial, "success") => Terminal
      case (S1, "success") => Terminal
      case (S2, "success") => Terminal
      case (Initial, "fail") => S1
      case (S1, "fail") => S2
      case (S2, "fail") => Matched
      case (Terminal, "fail") => S2
    }
  }
}