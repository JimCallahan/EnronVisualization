package org.spiffy.enron

/** An average of e-mail volume and sentiment over an time interval.
  * @constructor
  * @param sent The average number of e-mail messages sent.
  * @param terms The average occurrence of words in each of the FinancialTerm categories. */  
class AverageSentiment private (val sent: Double, terms: Array[Double]) {
  /** Get the average number of times a word in a particular FinancialTerm category was mentioned. */ 
  def total(term: FinancialTerm.Value) = terms(term.id)
  
  /** Get the frequency with which a word in a particular FinancialTerm category was mentioned. */ 
  def frequency(term: FinancialTerm.Value) = terms(term.id) / sent
  
  /** Get the average number of words in all e-mails sent. */
  def words = terms.reduce(_ + _)
  
  override def toString = 
    "AverageSentiment(sent=" + sent + ", total=(" + terms.map("%.4f".format(_)).reduce(_ + ", " + _) + "))"
    
  /** Convert to XML representation. */
  def toXML = <Terms>{ terms.map("%.6f".format(_)).reduce(_ + " " + _) }</Terms> 
}

object AverageSentiment {
  def apply() = new AverageSentiment(0.0, Array.fill(FinancialTerm.values.size)(0.0))
  def apply(snt: Sentiment, samples: Long) = {
    val terms = Array.fill(FinancialTerm.values.size)(0.0)
    for(t <- FinancialTerm.values)
      terms(t.id) = snt.total(t).toDouble / samples.toDouble
    new AverageSentiment(snt.sent.toDouble / samples.toDouble, terms) 
  }
}
