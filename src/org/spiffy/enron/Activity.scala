package org.spiffy.enron

/** Some e-mail activity.
  * @constructor
  * @param sent The number of messages sent.
  * @param recv The number of message received.
  */
class Activity protected (val sent: Long, val recv: Long) {
  /** The total number e-mails sent and received by the person. */
  def total = sent + recv

  /** Accumulate message counts. */
  def +(that: Activity) = Activity(sent + that.sent, recv + that.recv)

  /** Increment to the sent count. */
  def incSend = Activity(sent + 1, recv)

  /** Increment to the received count. */
  def incRecv = Activity(sent, recv + 1)

  override def toString = "Activity(sent=" + sent + ", recv=" + recv + ")"
}

object Activity {
  def apply() = new Activity(0L, 0L)
  def apply(sent: Long, recv: Long) = new Activity(sent, recv)
}
