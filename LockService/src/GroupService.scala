package rings

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.collection.mutable
import scala.util.Random

class RingCell(var prev: BigInt, var next: BigInt)
class RingMap extends scala.collection.mutable.HashMap[BigInt, RingCell]

/**
 * RingService is an example app service for the actor-based KVStore/KVClient.
 * This one stores RingCell objects in the KVStore.  Each app server allocates new
 * RingCells (allocCell), writes them, and reads them randomly with consistency
 * checking (touchCell).  The allocCell and touchCell commands use direct reads
 * and writes to bypass the client cache.  Keeps a running set of Stats for each burst.
 *
 * @param myNodeID sequence number of this actor/server in the app tier
 * @param numNodes total number of servers in the app tier
 * @param storeServers the ActorRefs of the KVStore servers
 * @param burstSize number of commands per burst
 */

//class RingServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
class GroupServer (val myNodeID: Int, val numNodes: Int, val numGroup: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
  implicit val timeout = Timeout(5 seconds)
  val generator = new scala.util.Random
  val cellstore = new KVClient(storeServers)
  val dirtycells = new AnyMap
  val localWeight: Int = 70
  val log = Logging(context.system, this)

  //opsMap : Incoming nodeId, corresponding seqId
  var opsMap = new mutable.HashMap[Int, Int]()
  var sendSeqId: Int = 0
  var stats = new Stats
  var allocated: Int = 0
  var endpoints: Option[Seq[ActorRef]] = None

  def receive() = {
    /**Receive from loadMaster**/
    case Prime() =>
      joinGroup()
    /**Receive from other servers**/
    case Msg(msg) =>
      checkCorrect(msg)
      checkOrder(msg)
    /**Receive from loadMaster**/
    case Command() =>
      incoming(sender)
      command()
    case View(e) =>
      endpoints = Some(e)
    /**Proxy Message**/
    case PMsg(pMessage) =>
      proxy(pMessage)
    /**Forward Message**/
    case FMsg(fmsg) =>
      forward(fmsg)
    /**Forward Report Message**/
    case CheckReport(checkResult) => {
      if (!checkResult) {
        stats.errors += 1
      }
    }
    case BackGroupInfo(set, msg) => {
      val list = endpoints.get
      for (server <- set) {
        list(server) ! Msg(msg)
      }
    }
  }

  private def proxy(pMessage: Message) = {
    val key = cellstore.hashForKey(myNodeID, 0)
    val value = directRead(key)
    val list = endpoints.get
    var checkResult = false
    if (value.isDefined) {
      val set = value.get
      if (pMessage.ctype == 0) {  // remove node
        set.remove(pMessage.nodeId)
      }
      if (pMessage.ctype == 1) {  // add node
        set.add(pMessage.nodeId)
      }
      if (pMessage.ctype == 2) {  // check node
        checkResult = set.contains(pMessage.nodeId)
        //if (!checkResult) reportError(pMessage, set)
        sender() ! CheckReport(checkResult)
      }
      if (pMessage.ctype == 3) {  // forward
        /**pMessage: ctype = 3, nodeId = groupID = C, groupId = C, origin = A, seqNum = ?, inter = true**/
        //1. return groupSet to intermediary (new copy or send directly)
//        val newSet = set.clone()
//        sender() ! BackGroupInfo(newSet, pMessage)
        //2. proxy send to every nodes in this group
        for (serve <- set) {
          list(serve) ! Msg(pMessage)
        }
      }
    }
  }

  private def reportError(msg: Message, set: mutable.HashSet[Int]): Unit = {
    //print(s"nodeId = ${msg.nodeId} find In: ")
    for (s <- set) {
      print(s + " ")
    }
    println()
  }

  private def joinGroup() = {
    val newGroup = generator.nextInt(numGroup)
    //!stats.group.contains(newGroup)
    if (true) {
      stats.joins += 1
      val proxyNum = newGroup % numGroup
      val list  = endpoints.get

      val pMessage = new Message
      pMessage.ctype = 1
      pMessage.nodeId = myNodeID

      list(proxyNum) ! PMsg(pMessage)
      stats.group += newGroup
    }
  }

  private def leaveGroup() = {
    stats.leaves += 1
    if (stats.group.nonEmpty) {
      val it = stats.group.iterator
      val list  = endpoints.get
      while (it.hasNext) {
        val groupNum = it.next()

        val pMessage = new Message
        pMessage.ctype = 0
        pMessage.nodeId = myNodeID

        val proxyNum = groupNum % numGroup
        list(proxyNum) ! PMsg(pMessage)
      }
      stats.group.clear()
    }
  }

  private def checkOrder(msg: Message): Unit = {
    val originId = msg.origin
    val seqNum = msg.seqNum
    val lastSeqId = opsMap.get(originId)
    if (lastSeqId.isEmpty) {
      opsMap.put(originId, seqNum)
    } else {
      if (lastSeqId.get >= seqNum) {
        stats.seqError += 1
        //println(s"myNodeId = $myNodeID get $lastSeqId > $seqNum")
      }
      opsMap.put(originId, seqNum)
    }
  }

  private def checkCorrect(msg: Message): Unit = {

    val proxyNum = msg.groupId % numGroup
    val list = endpoints.get

    val newMsg = msg.clone()
    newMsg.ctype = 2
    newMsg.nodeId = myNodeID

    list(proxyNum) ! PMsg(newMsg)
  }

  private def command() = {
    val sample = generator.nextInt(100)
    /**interSend: burst -> this is a interCommand, send: directly deliver**/
    if (sample < 50) {
      joinGroup()
    } else if (sample < 80){
      leaveGroup()
    } else if (sample < 85) {
      interSend()
    } else {
      send()
    }
  }

  /** A -> ![B]! -> C  I AM B!! msg: groupId = C, origin = A, seqNum = ?, inter = true**/
  private def forward(msg: Message) = {
    // get Group C's groupSet and send to all nodes in Group C
    val proxyNum = msg.groupId % numGroup
    val list  = endpoints.get
    /** Change fields **/
    val newMsg = msg.clone()
    newMsg.inter = false
    newMsg.ctype = 3
    newMsg.nodeId = msg.groupId

    list(proxyNum) ! PMsg(newMsg)
  }


  /** burst ->  this is a interCommand**/
  private def interSend() = {
    stats.resends += 1
    val list = endpoints.get
    val interServer = generator.nextInt(list.length)
    val groupNum = generator.nextInt(numGroup)

    val msg = new Message
    msg.inter = true
    msg.groupId = groupNum
    msg.seqNum = sendSeqId
    msg.origin = myNodeID
    sendSeqId += 1

    list(interServer) ! FMsg(msg)
  }
  private def send() = {
    stats.sends += 1
    val groupNum = generator.nextInt(numGroup)
    val list = endpoints.get
    val proxyNum = groupNum % numGroup

    val msg = new Message
    msg.ctype = 3
    msg.nodeId = myNodeID
    msg.inter = false
    msg.groupId = groupNum
    msg.seqNum = sendSeqId
    msg.origin = myNodeID
    sendSeqId += 1

    list(proxyNum) ! PMsg(msg)
  }

  private def incoming(master: ActorRef) = {
    stats.messages += 1
    if (stats.messages >= burstSize) {
      master ! BurstAck(myNodeID, stats)
      stats = new Stats
    }
  }

  private def directRead(key: BigInt): Option[scala.collection.mutable.HashSet[Int]] = {
    val result = cellstore.directRead(key)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[scala.collection.mutable.HashSet[Int]])
  }

  private def directWrite(key: BigInt, value: scala.collection.mutable.HashSet[Int]): Option[scala.collection.mutable.HashSet[Int]] = {
    val result = cellstore.directWrite(key, value)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[scala.collection.mutable.HashSet[Int]])
  }
}

object GroupServer {
  def props(myNodeID: Int, numNodes: Int, numGroup: Int, storeServers: Seq[ActorRef], burstSize: Int): Props = {
    Props(classOf[GroupServer], myNodeID, numNodes, numGroup, storeServers, burstSize)
  }
}
