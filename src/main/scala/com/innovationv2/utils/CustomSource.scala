package com.innovationv2.utils

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

import java.sql.Timestamp
import scala.util.Random

case class Event(id: Int, user: String, url: String, timestamp: Long)

abstract class EventSourceInterface[T](cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends SourceFunction[T] {
  var cur = 0
  val random = new Random
  var curTs = 0
  var id = 0
  val users: Array[String] = Array("Mary", "Bob", "Alice", "Cary")
  val urls: Array[String] = Array("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2")
  val sourceName: String

  def generateEventElement(): T

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    while (cur < cnt) {
      val event = this.generateEventElement()
      printEvent(event)
      ctx.collect(event)
      Thread.sleep(gap)
      cur += 1
    }
  }

  override def cancel(): Unit = cur = cnt

  def printEvent(event: T): Unit = {
    if (print)
      println(s"""Produce $sourceName: $event""")
  }
}

class BasicEventSource(cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends EventSourceInterface[Event](cnt, gap, print) {
  override val sourceName: String = "BasicEventSource"

  override def generateEventElement(): Event = {
    val username = users(random.nextInt(users.length))
    val url = urls(random.nextInt(urls.length))
    val event = Event(id, username, url, curTs)
    id += 1
    curTs += 1000
    event
  }
}

class EventSourceWithTimeStamp(cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends BasicEventSource(cnt, gap, print) {
  override val sourceName = "EventSourceWithTimeStamp"

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    while (cur < cnt) {
      val event = this.generateEventElement()
      printEvent(event)
      ctx.collectWithTimestamp(event, event.timestamp)
      Thread.sleep(1000L)
      cur += 1
    }
  }
}

class EventSourceWithWatermark(cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends BasicEventSource(cnt, gap, print) {
  override val sourceName = "EventSourceWithWatermark"

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    while (cur < cnt) {
      val event = this.generateEventElement()
      printEvent(event)
      ctx.collectWithTimestamp(event, event.timestamp)
      ctx.emitWatermark(new Watermark(event.timestamp - 1L))
      Thread.sleep(1000L)
      cur += 1
    }
  }
}

case class TimestampEvent(id: Int, user: String, url: String, timestamp: Timestamp)

class TimestampEventSource(cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends EventSourceInterface[TimestampEvent](cnt, gap, print) {
  override val sourceName = "TimestampEventSource"

  override def generateEventElement(): TimestampEvent = {
    val username = users(random.nextInt(users.length))
    val url = urls(random.nextInt(urls.length))
    val event = TimestampEvent(id, username, url, new Timestamp(curTs))
    id += 1
    curTs += 1000
    event
  }
}

case class LoginEvent(userId: String, ip: String, eventType: String, timestamp: Long)
class LoginEventSource extends SourceFunction[LoginEvent] {
  val user = Array("user_1", "user_2")
  var curTs = 1000L

  override def run(ctx: SourceFunction.SourceContext[LoginEvent]): Unit = {
    for (_ <- 0 to 8) {
      val loginEvent = LoginEvent(user(Random.nextInt(2)), "192.168.0.1", "fail", curTs)
      curTs += 1000L

      printEvent(loginEvent)
      ctx.collect(loginEvent)
      Thread.sleep(1000L)
    }
  }

  override def cancel(): Unit = ???

  private def printEvent(event: LoginEvent): Unit = {
    println(s"""Produce: $event""")
  }
}