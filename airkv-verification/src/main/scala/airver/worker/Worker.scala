package airver.worker

import airver._

import java.io.{BufferedReader, InputStreamReader}

object Worker {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      System.err.println("Miss \"ID\" for worker.")
      System.exit(1)
    }
    val worker = new Worker(args.head)
    val exitVal = worker.run()
    System.exit(exitVal)
  }
}

class Worker(val id: String) {

  val reader = new BufferedReader(new InputStreamReader(System.in))

  final def response(message: Message, seqNo: Int): Unit = {
    System.err.println(Message.msg2String(message, seqNo))
  }

  response(WorkerStart(), -1)

  def handleAbort(): Unit = {
    response(WorkerAbort(), -1)
    println("[Worker-%s] Handle ABORT".format(id))
    System.exit(0)
  }

  def handlePut(put: Put, seqNo: Int): Unit = {
    println("[Worker-%s] Handle PUT(%s,%s)".format(id, put.key, put.value))
    /// TODO: Wenwen,Hu

  }

  def handleGetAndCheck(getAndCheck: GetAndCheck, seqNo: Int): Unit = {
    println("[Worker-%s] Handle GET(%s)CHECK(%s)".format(id, getAndCheck.key, getAndCheck.expectedValue))
    /// TODO: WenWen,Hu

    // 返回检查结果
    response(WorkerExecResult(seqNo % 2 == 0), seqNo)
  }

  def run(): Int = {
    var line: String = null
    var aborted = false
    while (!aborted) {
      line = reader.readLine()
      if (line == null) {
        aborted = true
      } else {
        Message.string2Msg(line) match {
          case x: (_, Int) =>
            val seqNo = x._2
            x._1 match {
              case msg: Put => handlePut(msg, seqNo)
              case msg: GetAndCheck => handleGetAndCheck(msg, seqNo)
              case _: Abort => handleAbort()
              case _ => ???
            }
          case _ => ???
        }
      }
    }
    0
  }
}
