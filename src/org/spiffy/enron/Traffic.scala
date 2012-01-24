package org.spiffy.enron

/** A counter of the amount of directional e-mail activity between two people.
  * @constructor
  * @param sendID The personal identifier of the sender.
  * @param recvID The personal identifier of the receiver.
  * @param count The number of e-mails send between them.
  */
class Traffic private (val sendID: Long, val recvID: Long, val count: Long) 
  extends Ordered[Traffic] 
{
  /** Ordered in increasing activity and ascending IDs. */
  def compare(that: Traffic): Int =
    (count compare that.count) match {
      case 0 => 
        (sendID compare that.sendID) match {
          case 0 => recvID compare that.recvID
          case d => d
        }
      case c => c
    }
  
  /** Accumulate message counts. */
  def +(that: Traffic) = Traffic(sendID, recvID, count + that.count)

  /** Increment to the sent count. */
  def inc = Traffic(sendID, recvID, count + 1)

  override def toString = "Traffic(sendID=" + sendID + ", recvID=" + recvID + ", count=" + count + ")"
}

object Traffic {
  def apply(sendID: Long, recvID: Long) = new Traffic(sendID, recvID, 0L)
  def apply(sendID: Long, recvID: Long, count: Long) = new Traffic(sendID, recvID, count)
}
