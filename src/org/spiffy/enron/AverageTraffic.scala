package org.spiffy.enron

/** The average amount of directional e-mail activity between two people over a time interval.
  * @constructor
  * @param sendID The personal identifier of the sender.
  * @param recvID The personal identifier of the receiver.
  * @param count The average number of e-mails send between them during the sample period.
  */
class AverageTraffic private (val sendID: Long, val recvID: Long, val count: Double) {
  override def toString = "AverageTraffic(sendID=%d, recvID=%d, count=%.6f)".format(sendID, recvID, count)
}

object AverageTraffic {
  def apply(sendID: Long, recvID: Long) = new AverageTraffic(sendID, recvID, 0.0)
  def apply(traffic: Traffic, samples: Long) = 
    new AverageTraffic(traffic.sendID, traffic.recvID, traffic.count / samples.toDouble)
}
