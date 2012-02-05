package org.spiffy.enron

import scala.xml.Node

/** An average of e-mail volume and sentiment over an time interval.
  * @constructor
  * @param sent The average number of e-mail messages sent.
  * @param terms The average occurrence of words in each of the FinancialTerm categories.
  */
class AverageSentiment private (val sent: Double, val words: Double, terms: Array[Double]) {
  /** Accumulate message, word and term percentages. */
  def +(that: AverageSentiment) = {
    val ts = Array.fill(FinancialTerm.values.size)(0.0)
    for (t <- FinancialTerm.values)
      ts(t.id) = freq(t) + that.freq(t)
    new AverageSentiment(sent + that.sent, words + that.words, ts)
  }

  /** Normalize all terms by dividing them by the given total. */
  def normalize(total: Double): AverageSentiment = {
    if (total != 0.0) {
      for (i <- 0 until terms.size) {
        terms(i) = terms(i) / total
      }
    }
    this
  }

  /** Get the frequency with which a word in a particular FinancialTerm category was mentioned. */
  def freq(term: FinancialTerm.Value) = terms(term.id)

  override def toString =
    "AverageSentiment(sent=%.8f, word=%.8f".format(sent, words) + ", " +
      "total=(" + terms.map("%.8f".format(_)).reduce(_ + ", " + _) + "))"

  /** Convert to XML representation. */
  def toXML =
    <AvgSent sent={ "%.8f".format(sent) } words={ "%.8f".format(words) }>{
      terms.map("%.8f".format(_)).reduce(_ + " " + _)
    }</AvgSent>
}

object AverageSentiment {
  def apply() = new AverageSentiment(0.0, 0.0, Array.fill(FinancialTerm.values.size)(0.0))
  def apply(snt: Sentiment, scale: Double) = {
    val terms = Array.fill(FinancialTerm.values.size)(0.0)
    for (t <- FinancialTerm.values)
      terms(t.id) = scale * snt.total(t).toDouble
    new AverageSentiment(scale * snt.sent.toDouble, scale * snt.words.toDouble, terms)
  }

  /** Create from XML data. */
  def fromXML(node: Node): AverageSentiment = {
    val s = (node \ "@sent").text.toDouble
    val w = (node \ "@words").text.toDouble
    val terms = node.text.trim.split("\\p{Space}").map(_.toDouble)
    new AverageSentiment(s, w, terms)
  }
}
