package airver

import java.io.{BufferedReader, File, FileReader}
import java.util.concurrent.LinkedBlockingQueue
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

object WorkerMeta {
  type Status = Int
  val StatusUnknown = 0
  val StatusStart = 1
  val StatusAbort = 2
}

class WorkerMeta() {
  import WorkerMeta._

  @BeanProperty var id: String = _
  @BeanProperty var classpath: String =
      "./target/classes:" +
      "./target/airkv-verification-1.0-SNAPSHOT.jar:" +
      "./target/airkv-verification-1.0-SNAPSHOT-jar-with-dependencies.jar"
  @BeanProperty var mainClass: String = "airver.worker.Worker"
  @BeanProperty var programArguments: Array[String] = Array()
  @BeanProperty var jvmOptions: Array[String] = Array()

  @volatile var status: Status = StatusUnknown

  val messageIn = new LinkedBlockingQueue[String]()
  val stdOut = new LinkedBlockingQueue[String]()
  val stdErr = new LinkedBlockingQueue[String]()

  // Profile will be null in test mode
  def loadFromFile(profile: File) = {
    var reader: BufferedReader = null
    try {
      reader = new BufferedReader(new FileReader(profile))
      var r = reader.readLine()
      while (r != null) {
        val line = r.trim()
        if (!line.startsWith("#") && !line.startsWith("--")) {
          val token = line.split('=').map(_.trim)
          if (token.length == 2) {
            token(0).toLowerCase() match {
              case "id" => id = token(1)
              case "classpath" => classpath = token(1)
              case "mainClass" => mainClass = token(1)
              case "programArguments" => programArguments = token(1).split(',')
              case "jvmOptions" => jvmOptions = token(1).split(',')
              case _ => throw new Exception("Unknown line \"%s\"".format(line))
            }
          } else {
            throw new Exception("Unknown line \"%s\"".format(line))
          }
        }
        r = reader.readLine()
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }

  final def sendMessage(msg: Message, seqNo: Int) = {
    messageIn.put(Message.msg2String(msg, seqNo))
  }

  final def getCommand(): String = {
    val sb = new ArrayBuffer[String]()
    sb += "java"
    sb += "-cp"
    sb += classpath
    sb ++= jvmOptions
    sb += mainClass
    sb += id
    sb += programArguments.mkString(" ")
    sb.mkString(" ")
  }
}

