package org.spiffy.enron

import scala.xml.Node

/** The average amount and sentiment of directional e-mail activity between two people over a time interval.
  * @constructor
  * @param sendID The personal identifier of the sender.
  * @param recvID The personal identifier of the receiver.
  * @param send
  * @param recv
  */
class AverageBiSentiment private (val sendID: Long,
                                  val recvID: Long,
                                  val send: AverageSentiment,
                                  val recv: AverageSentiment)
  extends Ordered[AverageBiSentiment] {

  /** Ordered in decreasing number of words sent and ascending IDs. */
  def compare(that: AverageBiSentiment): Int =
    (that.words compare words) match {
      case 0 =>
        (sendID compare that.sendID) match {
          case 0 => recvID compare that.recvID
          case d => d
        }
      case c => c
    }

  /** Accumulate message, word and term percentages. */
  def +(that: AverageBiSentiment) =
    if ((sendID == that.sendID) && (recvID == that.recvID))
      new AverageBiSentiment(sendID, recvID, send + that.send, recv + that.recv)
    else
      throw new IllegalArgumentException("Only sentiment from the same sender/receiver can be combined!")

  /** Total number of e-mails sent in both directions. */
  def sent = send.sent + recv.sent

  /** Total number of words sent in both directions. */
  def words = send.words + recv.words

  override def toString =
    "AverageBiSentiment(sendID=" + sendID + ", recvID=" + recvID + ", send=" + send + ", recv=" + recv + ")"

  /** Convert to XML representation. */
  def toXML =
    if (sendID < recvID)
      <AvgBiSent sendID={ sendID.toString } recvID={ recvID.toString }>{ send.toXML }{ recv.toXML }</AvgBiSent>
    else
      <AvgBiSent sendID={ recvID.toString } recvID={ sendID.toString }>{ recv.toXML }{ send.toXML }</AvgBiSent>
}

object AverageBiSentiment {
  def apply(sendID: Long, recvID: Long) =
    new AverageBiSentiment(sendID, recvID, AverageSentiment(), AverageSentiment())
  def apply(sendID: Long, recvID: Long, send: AverageSentiment, recv: AverageSentiment) =
    new AverageBiSentiment(sendID, recvID, send, recv)

  /** Create from XML data. */
  def fromXML(node: Node): AverageBiSentiment = {
    val sid = (node \ "@sendID").text.toLong
    val rid = (node \ "@recvID").text.toLong
    val Seq(send, recv) = (node \ "AvgSent").map(AverageSentiment.fromXML(_))
    if (sid < rid)
      new AverageBiSentiment(sid, rid, send, recv)
    else
      new AverageBiSentiment(rid, sid, recv, send)
  }
}
