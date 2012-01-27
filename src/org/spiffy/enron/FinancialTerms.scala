package org.spiffy.enron

import org.scalagfx.io.Path

import collection.mutable.HashMap
import scala.xml.{ Elem, XML }

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader }

/** The classifications of English terms when used in financial industry documents. */
object FinancialTerm extends Enumeration {
  val Litigious, ModalStrong, ModalWeak, Negative, Positive, Uncertainty, Undefined = Value
}

/** A dictionary of financial terms. */
class FinancialDictionary private (dict: HashMap[String, FinancialTerm.Value]) {
  import FinancialTerm._

  /** Classify an English word into one of the financial term categories.  */
  def classify(word: String): FinancialTerm.Value = {
    val w = word.trim.toUpperCase
    if (w.length > 0) dict.getOrElse(w, Undefined) else Undefined
  }

  /** Convert to an XML representation. */
  def toXML: Elem = {
    def f(term: FinancialTerm.Value) = dict.filter { case (_, t) => t == term }.keys.map(t => (<Term>{ t.toString }</Term>))
    <FinancialDictionary>
      <Litigious>{ f(Litigious) }</Litigious>
      <ModalStrong>{ f(ModalStrong) }</ModalStrong>
      <ModalWeak>{ f(ModalWeak) }</ModalWeak>
      <Negative>{ f(Negative) }</Negative>
      <Positive>{ f(Positive) }</Positive>
      <Uncertainty>{ f(Uncertainty) }</Uncertainty>
    </FinancialDictionary>
  }
}

object FinancialDictionary {
  /** Load the financial dictionary from the original plain text files supplied by Bill McDonald
    * <http://www.nd.edu/~mcdonald/Word_Lists.html> as a ZIP archive and described in "When Is a
    * Liability Not a Liability?" <http://www.nd.edu/~tloughra/Liability.pdf>
    * @param dir The directory where the text files have been unzipped.
    */
  def loadText(dir: Path): FinancialDictionary = {
    import FinancialTerm._
    val dict = new HashMap[String, FinancialTerm.Value]

    def f(path: Path, term: FinancialTerm.Value) {
      term match {
        case Undefined =>
        case _ => {
          println("  Reading: " + path)
          val in = new BufferedReader(new FileReader(path.toFile))
          try {
            var done = false
            while (!done) {
              val line = in.readLine
              if (line == null)
                done = true
              else
                dict += (line.trim.toUpperCase -> term)
            }
          }
          finally {
            in.close
          }
        }
      }
    }

    for (term <- FinancialTerm.values)
      f(dir + ("ND_FinTerms_" + term + "_v2.txt"), term)

    new FinancialDictionary(dict)
  }

  /** Read the financial dictionary from XML file. */
  def loadXML(path: Path): FinancialDictionary = {
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      fromXML(XML.load(in))
    }
    finally {
      in.close
    }
  }

  /** Create a new financial dictionary from XML data. */
  def fromXML(elem: Elem): FinancialDictionary = {
    import FinancialTerm._
    val dict = new HashMap[String, FinancialTerm.Value]
    val terms = elem \\ "FinancialDictionary"
    for (t <- terms \\ "Litigious" \\ "Term")
      dict += t.text -> Litigious
    for (t <- terms \\ "ModalStrong" \\ "Term")
      dict += t.text -> ModalStrong
    for (t <- terms \\ "ModalWeak" \\ "Term")
      dict += t.text -> ModalWeak
    for (t <- terms \\ "Negative" \\ "Term")
      dict += t.text -> Negative
    for (t <- terms \\ "Positive" \\ "Term")
      dict += t.text -> Positive
    for (t <- terms \\ "Uncertainty" \\ "Term")
      dict += t.text -> Uncertainty

    new FinancialDictionary(dict)
  }
}