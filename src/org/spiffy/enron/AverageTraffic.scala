package org.spiffy.enron

/** The average amount of directional e-mail activity between two people over a time interval.
  * @constructor
  * @param sendID The personal identifier of the sender.
  * @param recvID The personal identifier of the receiver.
  * @param count The average number of e-mails send between them during the sample period.
  */
class AverageTraffic private (val sendID: Long, val recvID: Long, val count: Double)
  extends Ordered[AverageTraffic] 
{
  /** Ordered in increasing activity and ascending IDs. */
  def compare(that: AverageTraffic): Int =
    (count compare that.count) match {
      case 0 => 
        (sendID compare that.sendID) match {
          case 0 => recvID compare that.recvID
          case d => d
        }
      case c => c
    }

  override def toString = "AverageTraffic(sendID=%d, recvID=%d, count=%.6f)".format(sendID, recvID, count)
}

object AverageTraffic {
  def apply(sendID: Long, recvID: Long) = new AverageTraffic(sendID, recvID, 0.0)
  def apply(traffic: Traffic, samples: Long) = 
    new AverageTraffic(traffic.sendID, traffic.recvID, traffic.count / samples.toDouble)
}
