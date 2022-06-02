package airver

import java.util
import java.util.concurrent.CountDownLatch
import java.util.{Timer, TimerTask, HashMap => JavaHashMap}
import scala.collection.JavaConversions._

class Simulator {
  val controller = new Controller()
  private var idSeed: Int = 0

  private val workers = new JavaHashMap[String, WorkerMeta]()
  private val workerThreads = new JavaHashMap[String, ControlHandle]()

  def createDefaultWorker(): String = {
    val id = "test-%s".format(idSeed)
    idSeed += 1
    val wm = new WorkerMeta()
    wm.setId(id)
    workers.put(wm.id, wm)
    id
  }

  def launchAllWorkers(): Unit = {
    workers.foreach(kv => {
      val handle = controller.createWorkerControllerThread(kv._2)
      workerThreads.put(kv._1, handle)
      handle.start()
    })
  }

  def shutdownAllWorkers(): Unit = {
    workers.foreach(_._2.sendMessage(Abort(), -1))
    while (workers.exists(_._2.status != WorkerMeta.StatusAbort)) {
      Thread.sleep(1)
    }
  }

  case class RuntimeEvent(seqNo: Int, event: ScriptEvent) {
    var startTick: Int = 0
    var endTick: Int = -1
    var result: Boolean = true
    def timestamp: Int = event.timestamp
    def depends: Seq[Int] = event.depends
    def workerId: String = event.workerId
    def actionsMsg: Seq[Message] = event.actionsMsg

    def hasPassed(): Boolean = {
      if (endTick >= 0 && result) {
        true
      } else {
        false
      }
    }

    def resultString(): String = {
      if (endTick < 0) {
        "N/A"
      } else if (result) {
        "Yes"
      } else {
        "No"
      }
    }
  }

  def run(script: Script): Unit = {
    assert(workers.nonEmpty)
    assert(!workers.exists(_._2.status != WorkerMeta.StatusStart))

    val timer = new Timer()
    @volatile var tick: Int = 0

    val events = script.events
    val runtimeEventQueue = new util.LinkedList[RuntimeEvent]()
    val runtimeEventIdx = events.zipWithIndex.map(se => {
      val re = RuntimeEvent(se._2, se._1)
      runtimeEventQueue.addLast(re)
      re
    }).toArray

    val pendingEvents = new util.LinkedList[RuntimeEvent]()

    def isDependsReady(runtimeEvent: RuntimeEvent): Boolean = {
      runtimeEvent.depends.forall(dependsNo => runtimeEventIdx(dependsNo).hasPassed())
    }

    controller.resultCallback = (seqNo: Int, result: Boolean) => {
      if (seqNo >= 0) {
        val runtimeEvent = runtimeEventIdx(seqNo)
        runtimeEvent.result = result
        runtimeEvent.endTick = tick
      }
    }

    val cd = new CountDownLatch(1)
    timer.scheduleAtFixedRate(new TimerTask {
      override def run(): Unit = {
        if (runtimeEventQueue.isEmpty && pendingEvents.isEmpty) {
          cd.countDown()
        } else {
          val iterator = pendingEvents.iterator()
          while (iterator.hasNext) {
            val e = iterator.next()
            if (isDependsReady(e)) {
              e.actionsMsg.foreach(m => workers(e.workerId).sendMessage(m, e.seqNo))
              runtimeEventIdx(e.seqNo).startTick = tick
              iterator.remove()
            }
          }
          while (runtimeEventQueue.nonEmpty && runtimeEventQueue.head.timestamp <= tick) {
            val e = runtimeEventQueue.poll()
            if (isDependsReady(e)) {
              e.actionsMsg.foreach(m => workers(e.workerId).sendMessage(m, e.seqNo))
              runtimeEventIdx(e.seqNo).startTick = tick
            } else {
              pendingEvents.addLast(e)
            }
          }
        }
        tick += 10
      }
    }, 0, 10)
    cd.await()
    controller.pln("The script has been executed")
    timer.cancel()

    println()
    println("Report:")
    println(runtimeEventIdx.map(r => {
      "%s.[%s]: %s".format(r.seqNo, r.event, r.resultString())
    }).mkString("\n"))
  }

}
