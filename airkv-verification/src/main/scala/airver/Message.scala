package airver

object Message {
  val Splitter = "###"
  val Header = "C#"

  private def formatterToString(args: Any*): String = {
    Header + args.map(_.toString).mkString(Splitter)
  }

  def msg2String(msg: Message, seqNo: Int): String = {
    msg match {
      // Message
      case m: Put => formatterToString(m.id, seqNo, m.key, m.value)
      case m: GetAndCheck => formatterToString(m.id, seqNo, m.key, m.expectedValue)
      case m: Abort => formatterToString(m.id, seqNo)
      // Message Callbak
      case m: WorkerStart => formatterToString(m.id, seqNo)
      case m: WorkerAbort => formatterToString(m.id, seqNo)
      case m: WorkerExecResult => formatterToString(m.id, seqNo, m.result.toString)
      case _ => ???
    }
  }

  def isCommand(str: String): Boolean = str.startsWith(Header)

  def string2Msg(str: String): (Message, Int) = {
    assert(isCommand(str))
    val token = str.replaceFirst(Header, "").split(Splitter)
    val msgId = token(0).toInt
    val seqNo = token(1).toInt
    val msg = msgId match {
      // Message
      case 0 => Put(token(2), token(3))
      case 1 => GetAndCheck(token(2), token(3))
      case 99 => Abort()
      // Message Callbak
      case 1000 => WorkerStart()
      case 1001 => WorkerAbort()
      case 1002 => WorkerExecResult(token(2).toBoolean)
      case _ => ???
    }
    (msg, seqNo)
  }
}

// From main to worker
abstract class Message(val id: Int)
// From worker to main
abstract class MessageBack(id: Int) extends Message(id)

case class Put(key: String, value: String) extends Message(0)

case class GetAndCheck(key: String, expectedValue: String) extends Message(1)

case class Abort() extends Message(99)

case class WorkerStart() extends MessageBack(1000)
case class WorkerAbort() extends MessageBack(1001)
case class WorkerExecResult(result: Boolean) extends MessageBack(id = 1002)



