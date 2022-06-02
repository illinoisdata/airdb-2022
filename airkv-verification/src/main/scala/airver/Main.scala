package airver

object Main {

  private val simulator = new Simulator()
  private val worker1: String = simulator.createDefaultWorker()
  private val worker2: String = simulator.createDefaultWorker()

  def script1(): Script = {
    val script = new Script("Demo1")
    script.pushEvent(0, worker1, Put("1", "2"))
    script.pushEvent(10, worker2, GetAndCheck("1", "2"))
    script.pushEvent(20, worker1, Put("1", "3"))
    script.pushEvent(20, worker2, GetAndCheck("1", "2"))
    script.pushEvent(30, worker2, GetAndCheck("1", "3"))
    script
  }

  def main(args: Array[String]): Unit = {
    simulator.launchAllWorkers()
    val s = script1()
    println(s.dump())
    println()
    println("Run \"%s\"".format(s.name))
    simulator.run(s)
    simulator.shutdownAllWorkers()
  }

}
