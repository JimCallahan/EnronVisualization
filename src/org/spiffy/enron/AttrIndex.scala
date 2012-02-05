package org.spiffy.enron

import scala.xml.Node

/** An sortable attribute value and associated sender/receiver IDs. */
class AttrIndex private (val sendID: Long, val recvID: Long, val sendAttr: Double, val recvAttr: Double)
  extends Ordered[AttrIndex] {
  /** Ordered in decreasing attribute value and increasing send/recv IDs. */
  def compare(that: AttrIndex): Int =
    (that.total compare total) match {
      case 0 => (sendID compare that.sendID) match {
        case 0  => recvID compare that.recvID
        case c2 => c2
      }
      case c => c
    }

  /** Sum of both sender and receiver attribute values. */
  def total = sendAttr + recvAttr

  /** Convert to XML representation. */
  def toXML =
    <AttrIndex sendID={ sendID.toString } recvID={ recvID.toString }>{
      "%.8f %.8f".format(sendAttr, recvAttr)
    }</AttrIndex>
}

object AttrIndex {
  def apply(sendID: Long, recvID: Long, sendAttr: Double, recvAttr: Double) =
    new AttrIndex(sendID, recvID, sendAttr, recvAttr)

  /** Create from XML data. */
  def fromXML(node: Node): AttrIndex = {
    val sid = (node \ "@sendID").text.toLong
    val rid = (node \ "@recvID").text.toLong
    (node.text.trim.split("\\p{Space}").map(_.toDouble)) match {
      case Array(s, r) => new AttrIndex(sid, rid, s, r)
    }
  }
}
