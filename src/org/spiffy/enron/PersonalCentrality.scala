package org.spiffy.enron

/** The Eigenvector Centrality of a persons e-mail activity.
  * @constructor
  * @param pid The unique identifier (people.personid).
  * @param score The Eigenvector score computed for the person.
  */
class PersonalCentrality private (val pid: Long, val score: Double)
  extends PersonalIdentified
  with Ordered[PersonalCentrality] {
  /** Ordered in descending score and ascending IDs. */
  def compare(that: PersonalCentrality): Int =
    (that.score compare score) match {
      case 0 => pid compare that.pid
      case c => c
    }

  def normScore = scala.math.log(100.0 * score)

  override def toString = "PersonalCentrality(pid=%d, score=%.6f)".format(pid, score)
}

object PersonalCentrality {
  def apply(pid: Long) = new PersonalCentrality(pid, 0.0)
  def apply(pid: Long, score: Double) = new PersonalCentrality(pid, score)
}
