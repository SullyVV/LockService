package rings
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.Await

import java.security.Timestamp

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.util.Timeout
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.collection.mutable
class Ownership(var clientId: Int, var timestamp: Long)
class AckMsg(var clientId: Int, var file: String, var timestamp: Long, var made: Boolean)
class RecMsg(var file: String, var timestamp: Long)
class AcqMsg(var file: String, var clientId: Int, var timestamp: Long)
class RenMsg(var file: String, var clientId: Int, var T: Long)
class ReleaseMsg(var file: String, var clientId: Int, var T: Long)

//class RingServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
class LockServer ( var T: Long) extends Actor {
  // use a table to store filename and its ownership
  implicit val timeout = Timeout(60 seconds)
  val itime = T
  private val store = new scala.collection.mutable.HashMap[String, Ownership]
  val log = Logging(context.system, this)
  var table: Option[Seq[ActorRef]] = None
  def receive() = {
    case Init() =>
      init()
    case Acquire(msg) =>
      assign(msg)
    case Renew(msg) =>
      renew(msg)
    case ViewClient(e) =>
      table = Some(e)
  }
  private def check() = {
    // in this function we check every lease periodcally, if expires, we update our store and reclaim the lease from client
    // use scheduller
    // we have to think about this later, maybe ask someone
  }

  private def init() = {
    // init 1 file for test use
    store.put("file1", new Ownership(-1, 0))
    store.put("file2", new Ownership(-1, 0))
//    val future = ask(table.get(0), Test())
//    val done = Await.result(future, timeout.duration).asInstanceOf[String]
//    println(done)
    //println("init finished")
  }

  private def assign(msg: AcqMsg) = {
    val endpoints = table.get
    if (store(msg.file).clientId == -1) {
      // lease is empty, so use directly
      store.put(msg.file, new Ownership(msg.clientId, System.currentTimeMillis() + itime))
      sender() ! new AckMsg(-1, msg.file, store(msg.file).timestamp,true)
      //endpoints(msg.clientId) ! new AckMsg(msg.file, store(msg.file).timestamp,true)
    } else if (store(msg.file).clientId != -1 && store(msg.file).timestamp > System.currentTimeMillis()){
      // old lease still valid, check to see if lease is still in use
      val future = ask(endpoints(store(msg.file).clientId), Reclaim(new RecMsg(msg.file, System.currentTimeMillis())))
      val ackmsg = Await.result(future, timeout.duration).asInstanceOf[AckMsg]
      if (ackmsg.made == true) {
        // if lease not in use, assign it
        println("reclaim successful")
        store.put(msg.file, new Ownership(msg.clientId, System.currentTimeMillis() + itime))
        sender() ! new AckMsg(-1, msg.file, store(msg.file).timestamp, true)
      } else {
        // if lease in use, deny request
        sender() ! new AckMsg(-1, msg.file, store(msg.file).timestamp, false)
      }
    } else {
      // old lease expired and no renewal, allow requester and update
      store.put(msg.file, new Ownership(msg.clientId, System.currentTimeMillis() + itime))
      sender() ! new AckMsg(-1, msg.file, System.currentTimeMillis() + itime,true)
    }
  }

  private def renew(msg: RenMsg) = {
    // when a renew request coming in, server update file's lease and ack true
    store(msg.file).timestamp += msg.T
    sender() ! new AckMsg(-1, msg.file, store(msg.file).timestamp, true)
  }
}

object LockServer{
  def props( T: Long): Props = {
    Props(classOf[LockServer], T)
  }
}
