package rings

import java.util

import scala.collection.mutable

class Stats {
  var messages: Int = 0
//  var allocated: Int = 0
//  var checks: Int = 0
//  var touches: Int = 0
//  var misses: Int = 0
  var errors: Int = 0
  var seqError: Int = 0
  var joins: Int = 0
  var leaves: Int = 0
  var sends: Int = 0
  var resends: Int = 0
  var lastGroup: scala.collection.mutable.Queue[Int] = mutable.Queue()
  var group: scala.collection.mutable.HashSet[Int] = mutable.HashSet()

  def addLast (lastGroupNum: Int): Unit = {
    if (lastGroup.length >= 2) {
      lastGroup.dequeue()
    }
    lastGroup.enqueue(lastGroupNum)
  }

  def isLast (lastGroupNum: Int): Boolean = {
    lastGroup.contains(lastGroupNum)
  }

  def += (right: Stats): Stats = {
    messages += right.messages
    joins += right.joins
    leaves += right.leaves
    sends += right.sends
    resends += right.resends
    seqError += right.seqError
    errors += right.errors
	  this
  }

  override def toString: String = {
    s"Stats msgs=$messages joins=$joins leaves=$leaves sends=$sends resends=$resends seqError = $seqError err=$errors"
//    s"Stats msgs=$messages joins=$joins leaves=$leaves sends=$sends group=$group err=$errors"
  }
}
