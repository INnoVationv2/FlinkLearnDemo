package com.innovationv2.utils

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

import java.sql.Timestamp
import java.util.Calendar
import scala.util.Random;

case class Event(id: Int, user: String, url: String, timestamp: Long) {
  override def toString: String = {
    s"""{$id,$user,$url,${new Timestamp(timestamp)}}"""
  }
}

class BasicEventSource(var cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends SourceFunction[Event] {
  val random = new Random
  var id = 0
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
    while (addToCnt(-1) > 0) {
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

  def addToCnt(value: Int): Int = {
    val tmp = cnt
    cnt += value
    tmp
  }
}

class EventSourceWithTimeStamp(cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends BasicEventSource(cnt: Int, gap: Long, print: Boolean) {
  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    while (addToCnt(-1) > 0) {
      val event = this.generateEventElement()
      printEvent("EventSourceWithTimeStamp", event)
      ctx.collectWithTimestamp(event, event.timestamp)
      Thread.sleep(1000L)
    }
  }
}

class EventSourceWithWatermark(cnt: Int = Int.MaxValue, gap: Long = 1000L, print: Boolean = true) extends BasicEventSource(cnt: Int, gap: Long, print: Boolean) {
  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    while (addToCnt(-1) > 0) {
      val event = this.generateEventElement()
      printEvent("EventSourceWithWatermark", event)
      ctx.collectWithTimestamp(event, event.timestamp)
      ctx.emitWatermark(new Watermark(event.timestamp - 1L))
      Thread.sleep(1000L)
    }
  }
}