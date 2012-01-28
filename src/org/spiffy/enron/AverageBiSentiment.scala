package org.spiffy.enron

/** The average amount and sentiment of directional e-mail activity between two people over a time interval.
  * @constructor
  * @param sendID The personal identifier of the sender.
  * @param recvID The personal identifier of the receiver.
  * @param send 
  * @param recv 
  */
class AverageBiSentiment private (val sendID: Long, val recvID: Long, val send: AverageSentiment, val recv: AverageSentiment)
  extends Ordered[AverageBiSentiment] 
{
  /** Ordered in increasing activity and ascending IDs. */
  def compare(that: AverageBiSentiment): Int =
    (sent compare that.sent) match {
      case 0 => 
        (sendID compare that.sendID) match {
          case 0 => recvID compare that.recvID
          case d => d
        }
      case c => c
    }

  /** Total number of e-mails sent in both directions. */ 
  def sent = send.sent + recv.sent
  
  /** Total number of words sent in both directions. */ 
  def words = send.words + recv.words
  
  override def toString = "AverageBiSentiment(sendID=" + sendID + ", recvID=" + recvID + ", send=" + send + ", recv=" + recv + ")"
  
  /** Convert to XML representation. */
  def toXML = <AvgBiSent sendID={ sendID.toString } recvID={ recvID.toString }>{ send.toXML }{ recv.toXML }</AvgBiSent>
}

object AverageBiSentiment {
  def apply(sendID: Long, recvID: Long) = 
    new AverageBiSentiment(sendID, recvID, AverageSentiment(), AverageSentiment())
  def apply(sendID: Long, recvID: Long, send: AverageSentiment, recv: AverageSentiment) =
    new AverageBiSentiment(sendID, recvID, send, recv)
}
