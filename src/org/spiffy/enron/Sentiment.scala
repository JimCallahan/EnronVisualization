package org.spiffy.enron

/** Counts of the number of words mentioned in e-mails for each of the FinancialTerm categories. */
class Sentiment {
  /** The number of e-mail messages sent. */
  private var count = 0L

  /** Counters if the occurrence of words in each of the FinancialTerm categories. */
  private val terms = Array.fill(FinancialTerm.values.size)(0L)

  /** Accumulate message and term counts. */
  def +=(that: Sentiment) = {
    count = count + that.count
    for (i <- FinancialTerm.values.map(_.id))
      terms(i) = terms(i) + that.terms(i)
  }

  /** Increment to the number of e-mail messages sent. */
  def inc() = {
    count = count + 1
  }

  /** Increment a FinancialTerm category counts. */
  def inc(term: FinancialTerm.Value) =
    terms(term.id) = terms(term.id) + 1

  /** Get the number of e-mails sent. */
  def sent = count

  /** Get the total number of times a word in a particular FinancialTerm category was mentioned. */
  def total(term: FinancialTerm.Value) = terms(term.id)

  /** Get the total number of words in all e-mails sent. */
  def words = terms.reduce(_ + _)

  override def toString =
    "Sentiment(sent=" + sent + ", total=(" + terms.map(_.toString).reduce(_ + ", " + _) + "))"

  /** Convert to XML representation. */
  def toXML = <Sentiment sent={ sent.toString }>{ terms.map(_.toString).reduce(_ + " " + _) }</Sentiment>
}

object Sentiment {
  def apply() = new Sentiment()
}
