package rings

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

object TestHarness {
  val system = ActorSystem("Rings")
  val generator = new scala.util.Random()
  implicit val timeout = Timeout(60 seconds)
  val numNodes = 10
  val burstSize = 1000
  val opsPerNode = 10000
  val numGroup = numNodes
  // Service tier: create app servers and a Seq of per-node Stats
  val master = KVAppService(system, numNodes, numGroup, burstSize)

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    val s = System.currentTimeMillis
    runUntilDone
    val runtime = System.currentTimeMillis - s
    val throughput = (opsPerNode * numNodes)/runtime
    println(s"Done in $runtime ms ($throughput Kops/sec)")
    system.shutdown()
  }

  def runUntilDone() = {
    val future = ask(master, Join()).mapTo[Stats]
    master ! Start(opsPerNode)
    val done = Await.result(future, 60 seconds)
  }

}
