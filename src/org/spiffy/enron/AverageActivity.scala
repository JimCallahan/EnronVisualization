package org.spiffy.enron

/** An average of e-mail activity over an time interval.
  * @constructor
  * @param sent The average number of messages sent.
  * @param recv The average number of message received.
  */
class AverageActivity private (val sent: Double, val recv: Double) {
  /** The average total number e-mails sent and received by the person. */
  def total = sent + recv

  override def toString = "AverageActivity(sent=%.4f, recv=%.4f)".format(sent, recv)
}

object AverageActivity {
  def apply() = new AverageActivity(0.0, 0.0)
  def apply(act: Activity, samples: Long) =
    new AverageActivity(act.sent.toDouble / samples.toDouble, act.recv.toDouble / samples.toDouble)
}
