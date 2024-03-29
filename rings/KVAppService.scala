package rings

import akka.actor.{ActorSystem, ActorRef, Props}

sealed trait AppServiceAPI
case class Prime() extends AppServiceAPI
case class Command() extends AppServiceAPI
case class ViewClient(endpoints: Seq[ActorRef]) extends AppServiceAPI
case class View(endpoints: Seq[ActorRef]) extends AppServiceAPI
case class AskLease(file: String) extends AppServiceAPI
case class Init(fileNum: Int) extends AppServiceAPI
case class ViewServer(server: ActorRef) extends AppServiceAPI
case class Acquire(message: AcqMsg) extends AppServiceAPI
case class Renew(renew: RenMsg) extends AppServiceAPI
case class Disconnect(clientId: Int, timeLength: Long) extends AppServiceAPI
case class Reconnect(clientId: Int) extends AppServiceAPI
case class Check() extends AppServiceAPI
case class ReleaseLease(file: String) extends AppServiceAPI
case class AppRenew(file: String) extends AppServiceAPI
case class RenewCheck() extends AppServiceAPI
case class Reclaim(reclaim: RecMsg) extends AppServiceAPI
case class IsIdle() extends AppServiceAPI
case class ReportLease(fileName: String) extends  AppServiceAPI
case class Test() extends AppServiceAPI
case class Release(file: String) extends AppServiceAPI
/**
 * This object instantiates the service tiers and a load-generating master, and
 * links all the actors together by passing around ActorRef references.
 *
 * The service to instantiate is bolted into the KVAppService code.  Modify this
 * object if you want to instantiate a different service.
 */

object KVAppService {

  def apply(system: ActorSystem, numNodes: Int, ackEach: Int): ActorRef = {
    /** Storage tier: create K/V store servers */
    val stores = for (i <- 0 until numNodes)
      yield system.actorOf(KVStore.props(), "RingStore" + i)
    /** Service tier: create app servers */
    val servers = for (i <- 0 until numNodes)
      yield system.actorOf(RingServer.props(i, numNodes, stores, ackEach), "RingServer" + i)

    /** If you want to initialize a different service instead, that previous line might look like this:
      * yield system.actorOf(GroupServer.props(i, numNodes, stores, ackEach), "GroupServer" + i)
      * For that you need to implement the GroupServer object and the companion actor class.
      * Following the "rings" example.
      */


    /** Tells each server the list of servers and their ActorRefs wrapped in a message. */
    for (server <- servers)
      server ! View(servers)

    /** Load-generating master */
    val master = system.actorOf(LoadMaster.props(numNodes, servers, ackEach), "LoadMaster")
    master
  }
}

