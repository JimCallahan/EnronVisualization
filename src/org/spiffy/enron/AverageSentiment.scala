package org.spiffy.enron

/** An average of e-mail volume and sentiment over an time interval.
  * @constructor
  * @param sent The average number of e-mail messages sent.
  * @param terms The average occurrence of words in each of the FinancialTerm categories. */  
class AverageSentiment private (val sent: Double, val words: Double, terms: Array[Double]) {
  /** Accumulate message, word and term percentages. */
  def + (that: AverageSentiment) = {
    val ts = Array.fill(FinancialTerm.values.size)(0.0)
    for(t <- FinancialTerm.values) 
      ts(t.id) = freq(t) + that.freq(t)
    new AverageSentiment(sent +  that.sent, words + that.words, ts)
  }

  /** Scale the percentages by a given factor. */
  def * (scale: Double) = {
    val ts = Array.fill(FinancialTerm.values.size)(0.0)
    for(t <- FinancialTerm.values) 
      ts(t.id) = freq(t) * scale
    new AverageSentiment(sent * scale, words * scale, ts)
  }
  
  /** Get the frequency with which a word in a particular FinancialTerm category was mentioned. */ 
  def freq(term: FinancialTerm.Value) = terms(term.id) 
  
  override def toString = 
    "AverageSentiment(sent=%.6f, word=%.6f".format(sent, words) + ", total=(" + terms.map("%.6f".format(_)).reduce(_ + ", " + _) + "))"
    
  /** Convert to XML representation. */
  def toXML = <AvgSent sent={ "%.6f".format(sent) } words={ "%.6f".format(words) } >{ terms.map("%.6f".format(_)).reduce(_ + " " + _) }</AvgSent> 
}

object AverageSentiment {
  def apply() = new AverageSentiment(0.0, 0.0, Array.fill(FinancialTerm.values.size)(0.0))
  def apply(snt: Sentiment) = {
    val terms = Array.fill(FinancialTerm.values.size)(0.0)
    val w = snt.words.toDouble
    for(t <- FinancialTerm.values)
      terms(t.id) = if(w > 0.0) (100.0 * snt.total(t).toDouble) / w else 0.0
    new AverageSentiment(snt.sent, snt.words, terms)
  }
}
