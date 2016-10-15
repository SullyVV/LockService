package rings

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging

import scala.collection.mutable

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
    case CheckReport(checkResult) =>
      update(checkResult)
  }

  private def proxy(pMessage: Message) = {
    val key = cellstore.hashForKey(myNodeID, 0)
    val value = directRead(key)
    val list = endpoints.get
    var checkResult = false
    if (!value.isEmpty) {
      val set = value.get
      if (pMessage.ctype == 0) {  // remove node
        set.remove(pMessage.nodeId)
      }
      if (pMessage.ctype == 1) {  // add node
        set.add(pMessage.nodeId)
      }
      if (pMessage.ctype == 2) {  // check node
        checkResult = set.contains(pMessage.nodeId)
        list(pMessage.nodeId) ! CheckReport(checkResult)
      }
      if (pMessage.ctype == 3) {  // forward
        /**pMessage: ctype = 3, nodeId = groupID = C, groupId = C, origin = A, seqNum = ?, inter = true**/
        //1. return groupSet to intermediary (new copy or send directly)
        //2. proxy send to every nodes in this group
        for (serve <- set) {
          list(serve) ! Msg(pMessage)
        }
      }
    }
  }

  private def joinGroup() = {
    val newGroup = generator.nextInt(numGroup)
    if (!stats.group.contains(newGroup)) {
      stats.joins = stats.joins + 1
      val proxyNum = newGroup % numGroup
      val pMessage = new Message
      pMessage.ctype = 1
      pMessage.nodeId = myNodeID
      val list  = endpoints.get
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
      }
      opsMap.put(originId, seqNum)
    }
  }

  private def checkCorrect(msg: Message): Unit = {
      val proxyNum = msg.groupId % numGroup
      val list = endpoints.get
      msg.ctype = 2
      msg.nodeId = myNodeID
      list(proxyNum) ! PMsg(msg)
  }

  private def command() = {
    val sample = generator.nextInt(100)
    /**interSend: burst -> this is a interCommand, send: directly deliver**/
    if (sample < 25) {
      joinGroup()
    } else if (sample < 50){
      //leaveGroup()
    } else if (sample < 75) {
      //interSend()
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
    msg.inter = false
    msg.ctype = 3
    msg.nodeId = msg.groupId

    list(proxyNum) ! PMsg(msg)
  }

  private def update(checkResult: Boolean): Unit = {
    if (!checkResult) {
      stats.errors += 1
    }
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
    msg.inter = false
    msg.groupId = groupNum
    msg.seqNum = sendSeqId
    msg.origin = myNodeID
    sendSeqId += 1

    list(proxyNum) ! Msg(msg)
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
