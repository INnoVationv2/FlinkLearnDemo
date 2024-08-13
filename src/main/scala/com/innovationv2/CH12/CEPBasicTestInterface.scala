package com.innovationv2.CH12

import com.innovationv2.utils.{LoginEvent, LoginEventSource}
import org.apache.flink.streaming.api.scala._
import org.junit.Before

class CEPBasicTestInterface(addLoginEventSource: Boolean = true) {
  var env: StreamExecutionEnvironment = _
  var loginEventStream: KeyedStream[LoginEvent, String] = _

  @Before
  def init(): Unit = {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    if (addLoginEventSource)
      _addLoginEventSource()
  }

  private def _addLoginEventSource(): Unit = {
    loginEventStream = env.addSource(new LoginEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.userId)
  }
}
