package org.spiffy.enron

/** The average amount of directional e-mail activity between two people over a time interval.
  * @constructor
  * @param sendID The personal identifier of the sender.
  * @param recvID The personal identifier of the receiver.
  * @param send The average number of e-mails sent from sendID to recvID during the sample period.
  * @param recv The average number of e-mails sent from recvID to sendID during the sample period.
  */
class AverageBiTraffic private (val sendID: Long, val recvID: Long, val send: Double, val recv: Double)
  extends Ordered[AverageBiTraffic] 
{
  /** Ordered in increasing activity and ascending IDs. */
  def compare(that: AverageBiTraffic): Int =
    (total compare that.total) match {
      case 0 => 
        (sendID compare that.sendID) match {
          case 0 => recvID compare that.recvID
          case d => d
        }
      case c => c
    }

  /** Total number of emails sent in both directions. */ 
  def total = send + recv
  
  /** Bidirectional total (minimum of each direction) number of emails sent. */
  def bidir = send min recv
  
  override def toString = "AverageTraffic(sendID=%d, recvID=%d, send=%.6f, recv=%.6f)".format(sendID, recvID, send, recv)
}

object AverageBiTraffic {
  def apply(sendID: Long, recvID: Long) = new AverageBiTraffic(sendID, recvID, 0.0, 0.0)
  def apply(sendID: Long, recvID: Long, send: Double, recv: Double) = new AverageBiTraffic(sendID, recvID, send, recv)
}
