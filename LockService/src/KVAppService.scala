package rings

import akka.actor.{ActorRef, ActorSystem, Props}

import scala.collection.mutable

sealed trait AppServiceAPI
case class Prime() extends AppServiceAPI
case class Msg(msg: Message) extends AppServiceAPI
case class Command() extends AppServiceAPI
case class View(endpoints: Seq[ActorRef]) extends AppServiceAPI
case class PMsg(pMessage: Message) extends AppServiceAPI
case class CheckReport(checkResult: Boolean) extends  AppServiceAPI
case class BackGroupInfo(newSet: mutable.HashSet[Int], msg: Message) extends AppServiceAPI
case class FMsg(fMsg: Message) extends AppServiceAPI
/**
 * This object instantiates the service tiers and a load-generating master, and
 * links all the actors together by passing around ActorRef references.
 *
 * The service to instantiate is bolted into the KVAppService code.  Modify this
 * object if you want to instantiate a different service.
 */

object KVAppService {

  def apply(system: ActorSystem, numNodes: Int, numGroup: Int, ackEach: Int): ActorRef = {

    /** Storage tier: create K/V store servers */
    val stores = for (i <- 0 until numNodes)
      yield system.actorOf(KVStore.props(), "GroupStore" + i)

    /** Service tier: create app servers */
    val servers = for (i <- 0 until numNodes)
      yield system.actorOf(GroupServer.props(i, numNodes, numGroup, stores, ackEach), "GroupServer" + i)

    /** If you want to initialize a different service instead, that previous line might look like this:
      * yield system.actorOf(GroupServer.props(i, numNodes, stores, ackEach), "GroupServer" + i)
      * For that you need to implement the GroupServer object and the companion actor class.
      * Following the "rings" example.
      */
      /** create group tables**/
    val cellStore = new KVClient(stores)
    for (i <- 0 until numGroup) {

      val key = cellStore.hashForKey(i, 0)
      cellStore.directWrite(key, new scala.collection.mutable.HashSet[Int])
    }

      /** Tells each server the list of servers and their ActorRefs wrapped in a message. */
    for (server <- servers)
      server ! View(servers)

    /** Load-generating master */
    val master = system.actorOf(LoadMaster.props(numNodes, numGroup, servers, ackEach), "LoadMaster")
    master
  }
}

