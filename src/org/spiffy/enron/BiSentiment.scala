package org.spiffy.enron

/** A counter of the amount of directional e-mail activity and sentiment between two people.
  * @constructor
  * @param sendID The personal identifier of the sender.
  * @param recvID The personal identifier of the receiver.
  * @param send The amount and sentiment of e-mails sent from sendID to recvID during the sample period.
  * @param recv The amount and sentiment of e-mails sent from recvID to sendID during the sample period.
  */
class BiSentiment private (val sendID: Long, val recvID: Long, val send: Sentiment, val recv: Sentiment)
  extends Ordered[BiSentiment] {
  /** Ordered in increasing activity and ascending IDs. */
  def compare(that: BiSentiment): Int =
    (total compare that.total) match {
      case 0 =>
        (sendID compare that.sendID) match {
          case 0 => recvID compare that.recvID
          case d => d
        }
      case c => c
    }

  /** Total number of emails sent in both directions. */
  def total = send.sent + recv.sent

  override def toString =
    "BiSentiment(sendID=" + sendID + ", recvID=" + recvID + ", send=" + send + ", recv=" + recv + ")"

  /** Convert to XML representation. */
  def toXML =
    <AvgBiSent sendID={ sendID.toString } recvID={ recvID.toString }>{ send.toXML }{ recv.toXML }</AvgBiSent>
}

object BiSentiment {
  def apply(sendID: Long, recvID: Long) = new BiSentiment(sendID, recvID, Sentiment(), Sentiment())
  def apply(sendID: Long, recvID: Long, send: Sentiment, recv: Sentiment) = new BiSentiment(sendID, recvID, send, recv)
}
