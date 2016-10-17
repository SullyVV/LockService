package rings
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import scala.concurrent.{Await, Future}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.util.Timeout
// for cached lease, we have to mark whether it is actually being used, if yes, cannot be reclaimed before expire
// if not, can be reclaimed
class LeaseCondition (var timestamp: Long, var used: Boolean)
class LockClient (val clientId: Int, var timeStep: Long) extends Actor {
  // use a table to store filename and lease time
  private val cache = new scala.collection.mutable.HashMap[String, LeaseCondition]
  implicit val timeout = Timeout(60 seconds)
  val log = Logging(context.system, this)
  var disconnect = false
  var server: Option[ActorRef] = None
  var stats = new Stats
  def receive() = {
    case Take(file) =>
      acqLease(file)
    case Reclaim(msg) =>
      // the only case where client get
      reclaim(msg)
    case ViewServer(e) =>
      //println("get server info")
      server = Some(e)
    case Release(file) =>
      releaseLease(file)
    case AppRenew(file) =>
      renewLease(file)
  }
  private def reclaim(recMsg: RecMsg) = {
    val timestamp = cache(recMsg.file).timestamp
    val requiredtimestamp = recMsg.timestamp
    val currenttime = System.currentTimeMillis()
    if (cache(recMsg.file).timestamp < recMsg.timestamp || cache(recMsg.file).used == false) {
      // in this situation, it is already expired, clean directly
      // or application using this lease has released, but is still cached in lock client
      cache.put(recMsg.file, new LeaseCondition(0, false))
      sender() ! new AckMsg(clientId, recMsg.file, System.currentTimeMillis(), true)
    } else  {
      // in this case, application is still using the lease
      sender() ! new AckMsg(clientId, recMsg.file, System.currentTimeMillis(), false)
    }
  }

  private def acqLease(file: String) {
    // here we have to use ask, because we must hold and wait until we really get the lease
    val s = server.get
    if (cache.contains(file) && cache(file).timestamp < System.currentTimeMillis()) {
      // in this case we have cached the lease and it is still valid, so we dont have to consult the server.
      println(s"client $clientId success")
    } else {
      val future = ask(s, Acquire(new AcqMsg(file, clientId, System.currentTimeMillis())))
      val done = Await.result(future, timeout.duration).asInstanceOf[AckMsg]
      if (done.made == true) {
        // if server agrees the lease, update its cache
        cache.put(file, new LeaseCondition(done.timestamp, true))
        println(s"client $clientId success")
      } else {
        // else, do nothing
        println(s"client $clientId failed")
      }

    }
  }
  private def renewLease(file: String) = {
    // use ask pattern, same reason as above
    val renMsg = new RenMsg(file, clientId, timeStep)
    val future = ask(server.get, Renew(renMsg))
    val done = Await.result(future, timeout.duration).asInstanceOf[AckMsg]
    if (done.made == true) {
      cache.put(done.file, new LeaseCondition(done.timestamp, true))
      println(s"client $clientId renew success")
    } else {
      println(s"client $clientId renew failed")
    }
  }

  private def releaseLease(file: String) = {
    // application want me to release, but i actually cache it, if someone ask for it, i give them this lease
    println(s"client $clientId release $file")
    cache(file).used = false
  }
}

object LockClient {
  def props(clientId: Int, timeStep: Long): Props = {
    Props(classOf[LockClient], clientId, timeStep)
  }
}
