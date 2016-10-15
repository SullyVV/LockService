package rings

import java.util

import scala.collection.mutable
/**
  * Created by panme on 9/25/2016.
  */
class Message {
  var groupId: Int = -1
  var origin: Int = -1
  var seqNum: Int = -1
  var inter: Boolean = false

  /**old PMessage**/
  var ctype: Int = -1
  var nodeId: Int = -1

  override def clone(): Message = {
    val newMsg = new Message
    newMsg.groupId = this.groupId
    newMsg.origin  = this.origin
    newMsg.seqNum  = this.seqNum
    newMsg.inter   = this.inter
    newMsg.ctype   = this.ctype
    newMsg.nodeId  = this.nodeId
    newMsg
  }

  override def toString: String = {
    s"Message groupId=$groupId origin=$origin seqNum=$seqNum"
  }
}
