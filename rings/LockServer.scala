package rings
import akka.pattern.ask

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import java.security.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import ExecutionContext.Implicits.global
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.util.Timeout
import akka.dispatch

import scala.util.Random
import scala.concurrent.duration._
import scala.collection.mutable
class Ownership(var clientId: Int, var timestamp: Long)
class AckMsg(var clientId: Int, var fileName: String, var timestamp: Long, var result: Boolean)
class ReportMsg(var clientId: Int, var cachedLease: scala.collection.mutable.HashMap[String, LeaseCondition])
class RecMsg(var fileName: String, var timestamp: Long)
class RecAckMsg(var clientId: Int, var fileName: String, var modifiedTimes: Int, var timestamp: Long, var result: Boolean)
class AcqMsg(var fileName: String, var clientId: Int, var timestamp: Long)
class RenMsg(var fileName: String, var clientId: Int, var T: Long, var modifiedTimes: Int)
class ReleaseMsg(var fileName: String, var clientId: Int, var T: Long)

//class RingServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
class LockServer ( var T: Long) extends Actor {
  // use a table to leaseTable filename and its ownership
  implicit val timeout = Timeout(60 seconds)
  val itime = T
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  private val disconnectTable = new scala.collection.mutable.HashMap[Int, Long] //clientId, timeStamp
  private val leaseTable = new scala.collection.mutable.HashMap[String, Ownership]
  private val fileTable = new scala.collection.mutable.HashMap[String, Int]
  val log = Logging(context.system, this)
  var clientsTable: Option[Seq[ActorRef]] = None
  def receive() = {
    case Init(fileNum) =>
      init(fileNum)
    case Acquire(msg) =>
      acquire(msg)
    case Renew(msg) =>
      renew(msg)
    case Disconnect(clientID, timeLength) =>
      disconnect(clientID, timeLength)
    case Reconnect(clientId) =>
      reconnect(clientId)
    case Check() =>
      check()
    case ViewClient(e) =>
      clientsTable = Some(e)
  }

  /***
    * Server check mutual exclusive
    */
  private def check(): Unit = {
    val clients = clientsTable.get
    clients.foreach((client: ActorRef) => {
      val future = ask(client, ReportLease())
      val finalReport = Await.result(future, timeout.duration).asInstanceOf[ReportMsg]
      finalReport.cachedLease.foreach((pair: (String, LeaseCondition)) => {
        fileTable.put(pair._1, fileTable(pair._1) + pair._2.modifiedTimes)
      })
    })
    sender() ! fileTable
  }

  /***
    * Store all files in leaseTable, Mark unused lease as (-1, 0) <==> (ownership, timestamp)
    * Store all files in fileTable, which record modification times
    */
  private def init(fileNum : Int) = {
    // Store file info in leaseTable and fileTable
    for (i <- 0 until fileNum) {
      val filename = "file" + i
      leaseTable.put(filename, new Ownership(-1, 0))
      fileTable.put(filename, 0)
    }
  }


  /***
    * Server acquire lease to the requested client (check availability)
    *
    * @param acqMsg
    */
  private def acquire(acqMsg: AcqMsg): Unit = {
    if (disconnectTable.contains(acqMsg.clientId)) {
      return
    }
    val clients = clientsTable.get
    // No client occupies this lease, grant to the requester directly
    // fixme: debug println
    println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client ${acqMsg.clientId} ask ${acqMsg.fileName} lease which is on client ${leaseTable(acqMsg.fileName).clientId}")
    if (leaseTable(acqMsg.fileName).clientId == -1) {
      leaseTable.put(acqMsg.fileName, new Ownership(acqMsg.clientId, System.currentTimeMillis() + itime))
      sender() ! new AckMsg(-1, acqMsg.fileName, leaseTable(acqMsg.fileName).timestamp, true)
    }
    // Some one is taking this lease and still available
    else if (leaseTable(acqMsg.fileName).clientId != -1 && leaseTable(acqMsg.fileName).timestamp > System.currentTimeMillis()){
      // old lease still valid, check to see if lease is still in use
      try {
        val future = ask(clients(leaseTable(acqMsg.fileName).clientId), Reclaim(new RecMsg(acqMsg.fileName, System.currentTimeMillis())))
        val reclaimMsg = Await.result(future, timeout.duration).asInstanceOf[RecAckMsg]
        // if lease not in use, acquire it
        if (reclaimMsg.result == true) {
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client ${reclaimMsg.clientId} release ${reclaimMsg.fileName} lease. ${reclaimMsg.fileName} reclaim successful!")
          fileTable.put(acqMsg.fileName, fileTable(acqMsg.fileName) + reclaimMsg.modifiedTimes)
          leaseTable.put(acqMsg.fileName, new Ownership(acqMsg.clientId, System.currentTimeMillis() + itime))
          sender() ! new AckMsg(-1, acqMsg.fileName, leaseTable(acqMsg.fileName).timestamp, true)
        }
        // if lease in use, deny request
        else {
          sender() ! new AckMsg(-1, acqMsg.fileName, leaseTable(acqMsg.fileName).timestamp, false)
        }
      } catch {
        case timeout: TimeoutException =>  {
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    \033[32mclient ${leaseTable(acqMsg.fileName).clientId} timeout: \033[0mno response for un-expired reclaim ${acqMsg.fileName}")
          sender() ! new AckMsg(-1, acqMsg.fileName, leaseTable(acqMsg.fileName).timestamp, false)
        }
        case e: Exception => {
          e.printStackTrace()
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client ${leaseTable(acqMsg.fileName).clientId} unknown error: no response for un-expired reclaim ${acqMsg.fileName}")
          sender() ! new AckMsg(-1, acqMsg.fileName, leaseTable(acqMsg.fileName).timestamp, false)
        }
      }
    }
    // Some one is taking this lease but expired and no renewal
    // tell the original holder release, grant lease to requester
    else {
      val previousHolder = clients(leaseTable(acqMsg.fileName).clientId)
      leaseTable.put(acqMsg.fileName, new Ownership(acqMsg.clientId, System.currentTimeMillis() + itime))
      sender() ! new AckMsg(-1, acqMsg.fileName, System.currentTimeMillis() + itime, true)
      try {
        val future = ask(previousHolder, Reclaim(new RecMsg(acqMsg.fileName, acqMsg.timestamp)))
        val reclaimMsg = Await.result(future, timeout.duration).asInstanceOf[RecAckMsg]
        if (reclaimMsg.result == true) {
          fileTable.put(acqMsg.fileName, fileTable(acqMsg.fileName) + reclaimMsg.modifiedTimes)
        } else {
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} : \033[32mPossible Error: Client${leaseTable(acqMsg.fileName).clientId} refuse release expired file${acqMsg.fileName} lease\033[0m")
        }
      } catch {
        case timeout: TimeoutException => {
          leaseTable.clear()
          fileTable.clear()
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client ${leaseTable(acqMsg.fileName).clientId} timeout: no response for expired reclaim ${acqMsg.fileName}")
        }
        case e: Exception => {
          e.printStackTrace()
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client ${leaseTable(acqMsg.fileName).clientId} unknown error: no response for expired reclaim ${acqMsg.fileName}")
        }
      }
    }
  }

  /***
    * Server renews the requested lease
    *
    * @param msg
    */
  private def renew(msg: RenMsg): Unit  = {
    if (disconnectTable.contains(msg.clientId)) {
      return
    }
    // when a renew request coming in, server update file's lease and ack true
    if (leaseTable(msg.fileName).clientId == msg.clientId) {
      leaseTable(msg.fileName).timestamp += msg.T
      sender() ! new AckMsg(-1, msg.fileName, leaseTable(msg.fileName).timestamp, true)
    } else {
      sender() ! new AckMsg(-1, msg.fileName, leaseTable(msg.fileName).timestamp, false)
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :   \033[32mPossible Error:  client${msg.clientId} request renew file${msg.fileName} lease, which is not belong to it\033[0m")
    }
  }

  /***
    * Simulate a disconnection between server and the specific client, ignore all messages
    *
    * @param clientId
    * @param timeLength
    */
  private def disconnect(clientId: Int, timeLength: Long): Unit = {
    disconnectTable.put(clientId, System.currentTimeMillis() + timeLength)
    println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId disconnected!")
    context.system.scheduler.scheduleOnce(timeLength.millis, self, Reconnect(clientId))
  }

  /***
    * Auto called by disconnect, used to reconnect the client
    *
    * @param clientId
    */
  private def reconnect(clientId: Int): Unit = {
    disconnectTable.remove(clientId)
    println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId reconnected!")
  }
}

object LockServer{
  def props( T: Long): Props = {
    Props(classOf[LockServer], T)
  }
}
