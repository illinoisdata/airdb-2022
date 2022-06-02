package airver

import java.util

import scala.collection.JavaConversions._

case class ScriptEvent(timestamp: Int, workerId: String, actionsMsg: Seq[Message], depends: Seq[Int]) {

  override def toString: String = {
    "%s|%s|%s".format(timestamp, workerId, actionsMsg.map(_.getClass.getSimpleName).mkString(","))
  }
}

class Script(val name: String) {
  val events = new util.LinkedList[ScriptEvent]

  def pushEvent(timestamp: Int, workerId: String, actionsMsg: Seq[Message], depends: Seq[Int]): Int = {
    if (events.nonEmpty) {
      if (events.last.timestamp > timestamp) {
        throw new Exception("Last event %s is older than current event %s".format(events.last.timestamp, timestamp))
      }
    }
    if (depends.exists(_ >= events.length)) {
      throw new Exception("Can't depend a backward event")
    }
    events += ScriptEvent(timestamp, workerId, actionsMsg, depends)
    events.length - 1
  }

  def pushEvent(timestamp: Int, workerId: String, actionsMsg: Message): Unit = {
    pushEvent(timestamp, workerId, Seq(actionsMsg), Seq())
  }

  def pushEvent(timestamp: Int, workerId: String, actionsMsg: Message, depends: Seq[Int]): Unit = {
    pushEvent(timestamp, workerId, Seq(actionsMsg), depends)
  }

  def dump(): String = {
    val sb = new StringBuffer()
    sb.append(name).append(":\n")
    sb.append("[").append(events.mkString(",")).append("]")
    sb.toString
  }
}
