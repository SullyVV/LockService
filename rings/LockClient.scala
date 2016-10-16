package rings
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import scala.concurrent.{Await, Future}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.Await
// for cached lease, we have to mark whether it is actually being used, if yes, cannot be reclaimed before expire
// if not, can be reclaimed
class LeaseCondition (var timestamp: Long, var used: Boolean)
class LockClient (var server: ActorRef, var myRef: ActorRef, var timeStep: Long) extends Actor {
  // use a table to store filename and lease time
  private val cache = new scala.collection.mutable.HashMap[String, LeaseCondition]
  val log = Logging(context.system, this)
  var disconnect = false
  def receive() = {
    case Reclaim(msg) =>
      // the only case where client get
      reclaim(msg)
    case Disconnect() =>
      // server sends me a disconnect, so dont reply anything until i get a connect
      disconnect = true
    case Connect() =>
      disconnect = false
  }
  private def reclaim(recMsg: RecMsg) = {
    if (cache(recMsg.file).timestamp < System.currentTimeMillis() || cache(recMsg.file).used == false) {
      // in this situation, it is already expired, clean directly
      // or application using this lease has released, but is still cached in lock client
      cache.put(recMsg.file, new LeaseCondition(0, false))
      server ! Reclaim(true)
    } else  {
      // in this case, application is still using the lease
      server ! Reclaim(false)
    }
  }

  private def acqLease(acqMsg: AcqMsg): String= {
    // here we have to use ask, because we must hold and wait until we really get the lease
    if (cache.contains(acqMsg.file) && cache(acqMsg.file).timestamp < System.currentTimeMillis()) {
      // in this case we have cached the lease and it is still valid, so we dont have to consult the server.
      return acqMsg.file;
     // do nothing but prints out
    } else {
      val future = ask(server, Acquire(acqMsg))
      val done = Await.result(future, 60 seconds)
      if (done.made == true) {
        // if server agrees the lease, update its cache
        cache.put(acqMsg.file, new LeaseCondition(acqMsg.timestamp + acqMsg.period, true))
        return acqMsg.file
      } else {
        // else, do nothing
        return "failed"
      }

    }
  }
  private def renewLease(renMsg: RenMsg) = {
    // use ask pattern, same reason as above
    val future = ask(server, Renew(renMsg))
    val done = Await.result(future, 60 seconds)
    if (done == true) {
      var timestamp = cache(renMsg.file).timestamp
      timestamp = timestamp + timeStep
      cache.put(renMsg.file, new LeaseCondition(timestamp, true))
    }
  }
  private def releaseLease(relMsg: RelMsg) = {
    // application want me to release, but i actually cache it, if someone ask for it, i give them this lease
    cache(relMsg.file).used = false
  }
}

object LockClient {
  def props(server: ActorRef, myRef: ActorRef, timeStep: Long): Props = {
    Props(classOf[LockClient], server, myRef, timeStep)
  }
}
