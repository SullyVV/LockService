package rings
import java.text.SimpleDateFormat

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import java.util.Date
import java.util.concurrent.TimeoutException
import scala.concurrent.{Await, ExecutionContext, Future}
import akka.pattern.{AskTimeoutException, ask}

import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.util.Timeout
// for cached lease, we have to mark whether it is actually being used, if yes, cannot be reclaimed before expire
// if not, can be reclaimed
class LeaseCondition (var timestamp: Long, var used: Boolean)

class LockClient (val clientId: Int, serve: ActorRef, var timeStep: Long) extends Actor {
  // use a table to store filename and lease time
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  private val cache = new scala.collection.mutable.HashMap[String, LeaseCondition]
  implicit val timeout = Timeout(5 seconds)
  val log = Logging(context.system, this)
  var disconnect = false
//  var server: Option[ActorRef] = None
  var server = serve
  var stats = new Stats
  def receive() = {
    case AskLease(file) =>
      appAskLease(file)
    case ReleaseLease(file) =>
      appReleaseLease(file)

    case RenewCheck() =>
      renewCheck()
    case ReportLease() =>
      reportLease()
    case Reclaim(msg) =>
      // server calls clients
      reclaim(msg)
  }

  /***
    * Send back cached files list to server for mutual exclusion checking
    */
  private def reportLease(): Unit = {
    val cachedFiles = cache.keySet
    sender() ! new ReportMsg(clientId, cachedFiles)
  }

  /***
    * Check cached leases, if remaining time less than 20% and still in use, renew the lease
    */
  private def renewCheck(): Unit = {
    cache.foreach((lease: (String, LeaseCondition)) => {
      // remaining time less than 20%
      if (lease._2.used && lease._2.timestamp > System.currentTimeMillis() && lease._2.timestamp - System.currentTimeMillis() <= 2000) {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId auto renew ${lease._1} lease")
        renewLease(lease._1)
      }
    })
  }

  /***
    * Server calls back lease
    *
    * @param recMsg
    */
  private def reclaim(recMsg: RecMsg) : Unit = {
    if (cache.get(recMsg.fileName).isEmpty) {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    \033[32mserver reclaims ${recMsg.fileName} but it's not in client $clientId\033[0m")
      return
    }
    val timestamp = cache(recMsg.fileName).timestamp
    val requiredTimestamp = recMsg.timestamp
    //val currenttime = System.currentTimeMillis()
    if (timestamp < requiredTimestamp || cache(recMsg.fileName).used == false) {
      // In this situation, it is already expired, clean directly
      // or application using this lease has released, but is still cached in lock client
      // cache.put(recMsg.fileName, new LeaseCondition(0, false))
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    Server reclaim ${recMsg.fileName} on client $clientId success")
      cache.remove(recMsg.fileName)
      sender() ! new AckMsg(clientId, recMsg.fileName, System.currentTimeMillis(), true)
    } else  {
      // in this case, application is still using the lease
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    Server reclaim ${recMsg.fileName} on client $clientId fail")
      sender() ! new AckMsg(clientId, recMsg.fileName, System.currentTimeMillis(), false)
    }
  }

  /***
    * Application calls client to acquire the specific file lease
    *
    * @param fileName
    */
  private def acqLease(fileName: String) {
    // use ask here, because we must hold and wait until we really get the lease
    val s = server
    if (cache.contains(fileName) && cache(fileName).timestamp > System.currentTimeMillis()) {
      // in this case we have cached the lease and it is still valid, so we don't have to consult the server.
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId find $fileName lease in cache success")
    } else {
      try {
        val future = ask(s, Acquire(new AcqMsg(fileName, clientId, System.currentTimeMillis())))
        val done = Await.result(future, timeout.duration).asInstanceOf[AckMsg]
        if (done.result == true) {
          // if server agrees the lease, update its cache
          // TODO: provide API to emulate App operations and useLease?
          cache.put(fileName, new LeaseCondition(done.timestamp, true))
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId acquire $fileName lease success")
        } else {
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId acquire $fileName lease failed")
        }
      } catch  {
        case timeout: TimeoutException => println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    " + s"\033[31mclient $clientId timeout\033[0m" + s": ask for $fileName's lease")
        case e: Exception => {
          e.printStackTrace()
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId unknown exception")
        }
      }
    }
  }

  /**
    * Client renews lease
    *
    * @param fileName
    */
  private def renewLease(fileName: String) = {
    // use ask pattern, same reason as above
    if (cache.get(fileName).isDefined) {
      val renMsg = new RenMsg(fileName, clientId, timeStep)
      try {
        val future = ask(server, Renew(renMsg))
        val done = Await.result(future, timeout.duration).asInstanceOf[AckMsg]
        if (done.result == true) {
          cache.put(done.fileName, new LeaseCondition(done.timestamp, false))
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId renew success")
        } else {
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId renew failed")
        }
      } catch {
        case timeout : TimeoutException => println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    " + s"\033[31mclient $clientId timeout\033[0m" + s": renew $fileName's lease")
        case e: Exception => {
          e.printStackTrace()
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId unknown exception")
        }
      }
    } else {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} :    client $clientId wants to renew $fileName which is not belong to it")
    }
  }

  /**
    * Simulate application releases its lease
    *
    * @param fileName
    */
  private def appReleaseLease(fileName: String) = {
    // application want me to release, but i actually cache it, if someone ask for it, i give them this lease
    if (cache.get(fileName).isDefined) {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} : Application on client $clientId release $fileName")
      cache(fileName).used = false
    } else {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} : \033[32mApplication on client $clientId wants to release $fileName's lease which is not belong to it\033[0m")
    }
  }

  /**
    * Simulate application asks for lease
    *
    * @param fileName
    */
  private def appAskLease(fileName: String): Unit = {
    // if the lease is in the cache: expired or not; but no other clients(apps) ask for it
    println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} : Application $clientId asks for $fileName lease")

    if (cache.get(fileName).isDefined) {
      // if lease expired, renew it
      if (cache.get(fileName).get.timestamp < System.currentTimeMillis()) {
        renewLease(fileName)
      } else {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} : Application $clientId find $fileName lease in Cache")
      }
    }
    // client doesn't cache the lease, acquire it from server
    else {
      acqLease(fileName)
    }

    // if get the lease success
    if (cache.get(fileName).isDefined && cache.get(fileName).get.timestamp > System.currentTimeMillis()) {
      cache.get(fileName).get.used = true
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} : Application $clientId asks for $fileName lease success")
      //TODO: may add some code to auto release lease
    } else {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))} : Application $clientId asks for $fileName lease fail")
    }
  }
}

object LockClient {
  def props(clientId: Int, serve: ActorRef, timeStep: Long): Props = {
    Props(classOf[LockClient], clientId, serve, timeStep)
  }
}
