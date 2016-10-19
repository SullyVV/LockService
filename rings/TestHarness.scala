package rings

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import ExecutionContext.Implicits.global

object TestHarness {
//  val system = ActorSystem("Rings")
//  implicit val timeout = Timeout(5 seconds)
//  val numNodes = 10
//  val burstSize = 1000
//  val opsPerNode = 10000
//  // Service tier: create app servers and a Seq of per-node Stats
//  val master = KVAppService(system, numNodes, burstSize)
//  def main(args: Array[String]): Unit = run()
//
//  def run(): Unit = {
//    val s = System.currentTimeMillis
//    runUntilDone
//    val runtime = System.currentTimeMillis - s
//    val throughput = (opsPerNode * numNodes)/runtime
//    println(s"Done in $runtime ms ($throughput Kops/sec)")
//    system.shutdown()
//  }
//
//  def runUntilDone() = {
//    master ! Start(opsPerNode)
//    val future = ask(master, Join()).mapTo[Stats]
//    val done = Await.result(future, timeout.duration)
//  }
  val clientNum = 5
  val system = ActorSystem("Rings")
  implicit val timeout = Timeout(60 seconds)
  val lockServer = system.actorOf(LockServer.props(10000), "lockServer")

  val lockClients = for (i <- 0 until clientNum)
    yield system.actorOf(LockClient.props(i, lockServer, 10000), "lockClient" + i)

  /***
    * Set two schedulers, one for clients auto check; another one for server check
    */
  for (i <- 0 until clientNum) {
    system.scheduler.schedule(3 seconds, 5 seconds, lockClients(i), RenewCheck())
  }
  //system.scheduler.schedule(10 seconds, 10 seconds, lockServer, Check())

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    lockServer ! ViewClient(lockClients)
    lockServer ! Init()

    /**
      * Simulate App's Operations
      * lockClients ! AskLease(fileName)
      * lockClients ! ReleaseLease(fileName)
      *
      * lockServer ! Disconnect(clientId, timeLength)
      */

    lockClients(0) ! AskLease("file1")
    Thread.sleep(50)
    lockClients(1) ! AskLease("file2")
    Thread.sleep(50)
    lockServer ! Disconnect(1, 5000)
    Thread.sleep(50)
    lockClients(1) ! ReleaseLease("file2")
    Thread.sleep(50)
    lockClients(0) ! AskLease("file2")
    Thread.sleep(50)
    lockClients(1) ! AskLease("file1")
//    Thread.sleep(5000)
//    lockClients(0) ! ReleaseLease("file1")
//    lockClients(0) ! AskLease("file2")
//    Thread.sleep(2000)
//
//    lockClients(0) ! AskLease("file1")
//    Thread.sleep(3000)
//
//    lockClients(1) ! AskLease("file1")


    try {
      val future = ask(lockServer, Check())
      val checkReport = Await.result(future, 60 second).asInstanceOf[AckMsg]
    } catch {
      case e : Exception => e.printStackTrace()
    }

    system.shutdown()
  }
}
