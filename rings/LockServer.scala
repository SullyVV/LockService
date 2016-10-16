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
class Ownership(var rqst: ActorRef, var timestamp: Long)
class AckMsg(var file: String, var timestamp: Long, var made: Boolean)
class RecMsg(var file: String)
class AcqMsg(var file: String, var rqst: ActorRef)
class RenMsg(var file: String, var rqst: ActorRef, var T: Long)

//class RingServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
class LockServer (var myself: ActorRef, var lockClient: Seq[ActorRef], var T: Long) extends Actor {
  // use a table to store filename and its ownership
  val itime = T
  private val store = new scala.collection.mutable.HashMap[String, Ownership]
  val log = Logging(context.system, this)
  def receive() = {
    case Acquire(msg) =>
      assign(msg)
    case Renew(msg) =>
      renew(msg)

    // actually no release request to server, client functions as a middleware
  }
  private def check() = {
    // in this function we check every lease periodcally, if expires, we update our store and reclaim the lease from client
    // use scheduller
  }


  private def assign(msg: AcqMsg) = {
    if (!store.contains(msg.file)) {
      // lease is empty, so use directly
      store.put(msg.file, new Ownership(msg.rqst, System.currentTimeMillis() + itime))
      msg.rqst ! new AckMsg(msg.file, store(msg.file).timestamp,true)
    } else if (store.contains(msg.file) && store(msg.file).timestamp > System.currentTimeMillis()){
      // old lease still valid, deny requester, check to see if lease is still in use
      val future = ask(store(msg.file).rqst, Reclaim(new RecMsg(msg.file)))
      val ackmsg = Await.result(future, 5 seconds).asInstanceOf[AckMsg]
      if (ackmsg.made == true) {
        // if lease not in use, assign it
        store.put(msg.file, new Ownership(msg.rqst, System.currentTimeMillis() + itime))
        msg.rqst ! new AckMsg(msg.file, store(msg.file).timestamp, true)
      } else {
        // if lease in use, deny request
        msg.rqst ! new AckMsg(msg.file, store(msg.file).timestamp, false)
      }
    } else {
      // old lease expired and no renewal, allow requester and update
      store.put(msg.file, new Ownership(msg.rqst, System.currentTimeMillis() + itime))
      msg.rqst ! new AckMsg(msg.file, System.currentTimeMillis() + itime,true)
    }
  }
  private def renew(msg: RenMsg) = {
    // when a renew request coming in, server update file's lease and ack true
    store(msg.file).timestamp += msg.T
    msg.rqst ! new AckMsg(msg.file, System.currentTimeMillis(), true)
  }
}

object LockServer{
  def props(mySelf: ActorRef,lockClient: Seq[ActorRef]): Props = {
    Props(classOf[LockServer], mySelf, lockClient)
  }
}
