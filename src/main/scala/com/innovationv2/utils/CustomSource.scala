package com.innovationv2.utils

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

import java.util.Calendar
import scala.util.Random;

case class Event(id: Int, user: String, url: String, timestamp: Long)

class BasicEventSource(var cnt: Int = Int.MaxValue, var gap: Long = 1000L, var print: Boolean = false) extends SourceFunction[Event] {
  val random = new Random
  private var id = 0
  val users: Array[String] = Array("Mary", "Bob", "Alice", "Cary")
  val urls: Array[String] = Array("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2")

  def generateEventElement(): Event = {
    val curTs = Calendar.getInstance.getTimeInMillis
    val username = users(random.nextInt(users.length))
    val url = urls(random.nextInt(urls.length))
    val event = Event(id, username, url, curTs)
    id += 1
    event
  }

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    for (_ <- 1 to cnt) {
      val event = this.generateEventElement()
      printEvent("BasicEventSource", event)
      ctx.collect(event)
      Thread.sleep(gap)
    }
  }

  override def cancel(): Unit = cnt = 0

  def printEvent(Prefix: String, event: Event): Unit = {
    if (print)
      println(s"""$Prefix: $event""")
  }
}

class EventSourceWithTimeStamp extends BasicEventSource {
  def this(print: Boolean) = {
    this()
    this.print = print
  }

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    for (_ <- 1 to cnt) {
      val event = this.generateEventElement()
      printEvent("EventSourceWithTimeStamp", event)
      ctx.collectWithTimestamp(event, event.timestamp)
      Thread.sleep(1000L)
    }
  }
}

class EventSourceWithWatermark extends BasicEventSource {
  def this(print: Boolean) = {
    this()
    this.print = print
  }

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    for (_ <- 1 to cnt) {
      val event = this.generateEventElement()
      printEvent("EventSourceWithWatermark", event)
      ctx.collectWithTimestamp(event, event.timestamp)
      ctx.emitWatermark(new Watermark(event.timestamp - 1L))
      Thread.sleep(1000L)
    }
  }
}