package airver

import java.io.{BufferedReader, File, InputStreamReader}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import scala.sys.process.{Process, ProcessIO}

object Controller {

  val CPUSaver: Boolean = true

  def main(args: Array[String]): Unit = {
    val worker1 = new WorkerMeta()
    worker1.setId("test1")
    val controller = new Controller()
    val worker1Handle = controller.createWorkerControllerThread(worker1)
    worker1Handle.start()

    Thread.sleep(100)
    worker1.messageIn.put(Message.msg2String(Put("1", "2"), -1))
    Thread.sleep(5000)
    worker1.messageIn.put(Message.msg2String(Abort(), -1))

    worker1Handle.join()

    println("OKKKK")
  }
}

case class ControlHandle(private val thread: Thread, private val cd: CountDownLatch) {
  def join() = thread.join()

  def start() = {
    thread.start()
    cd.await()
  }
}

class Controller() {

  import Controller._

  var resultCallback: (Int, Boolean) => Unit = _

  def pln(string: String): Unit = {
    println("[Controller] %s".format(string))
  }
  def pln(worker: WorkerMeta, string: String): Unit = {
    println("[Controller][Worker-%s] %s".format(worker.id, string))
  }

  val WorkerMap = new ConcurrentHashMap[String, WorkerMeta]()

  def createWorkerControllerThread(id: String, profile: File): ControlHandle = {
    val wm = new WorkerMeta()
    wm.setId("test1")
    createWorkerControllerThread(wm)
  }

  def createWorkerControllerThread(workerMeta: WorkerMeta): ControlHandle = {
    val cd = new CountDownLatch(1)
    val runnable = new Runnable {
      override def run(): Unit = {
        val worker = workerMeta
        WorkerMap.put(worker.id, worker)

        val command = worker.getCommand()
        println(command)
        val processBuilder = Process(command)

        val io = new ProcessIO(
          out => {
            while (worker.status != WorkerMeta.StatusAbort) {
              try {
                val msgStr = worker.messageIn.take() + "\n"
                out.write(msgStr.getBytes("utf-8"))
                out.flush()
              } catch {
                case _: Throwable =>
              }
            }
          },
          in => {
            val br = new BufferedReader(new InputStreamReader(in))
            while (worker.status != WorkerMeta.StatusAbort) {
              val line = br.readLine()
              if (line != null) {
                println(line)
              }
              if (CPUSaver) {
                Thread.sleep(1)
              }
            }
          },
          in => {
            val br = new BufferedReader(new InputStreamReader(in))
            while (worker.status != WorkerMeta.StatusAbort) {
              val line = br.readLine()
              if (line != null) {
                if (Message.isCommand(line)) {
                  Message.string2Msg(line) match {
                    case x: (_, Int) =>
                      val seqNo = x._2
                      x._1 match {
                        case _: WorkerStart =>
                          worker.status = WorkerMeta.StatusStart
                          cd.countDown()
                        case _: WorkerAbort =>
                          worker.status = WorkerMeta.StatusAbort
                        case x: WorkerExecResult =>
                          if (seqNo >= 0) {
                            resultCallback(seqNo, x.result)
                          }
                        case _ => ???
                      }
                    case _ => ???
                  }
                } else {
                  System.err.println(line)
                }
              }
              if (CPUSaver) {
                Thread.sleep(1)
              }
            }
          })

        pln(worker, "Start")
        val proc = processBuilder.run(io)
        while (worker.status != WorkerMeta.StatusAbort) {
          Thread.sleep(10)
        }
      }
    }
    val thread = new Thread(runnable, "Worker-monitor-%s".format(workerMeta.id))
    ControlHandle(thread, cd)
  }
}

