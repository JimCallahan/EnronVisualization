package org.spiffy.enron

import scala.xml.Node

/** The Eigenvector Centrality of a persons e-mail activity.
  * @constructor
  * @param pid The unique identifier (people.personid).
  * @param score The Eigenvector score computed for the person.
  */
class PersonalCentrality private (val pid: Long, val score: Double)
  extends PersonalIdentified
  with Ordered[PersonalCentrality] {
  /** Ordered by ascending personal IDs. */
  def compare(that: PersonalCentrality): Int = pid compare that.pid

  /** Log space score. */
  def normScore = scala.math.log(100.0 * score)

  override def toString = "PersonalCentrality(pid=%d, score=%.6f)".format(pid, score)
  
  /** Convert to XML representation. */ 
  def toXML = <PersonalCentrality><ID>{ pid }</ID><Score>{ "%.8f".format(score) }</Score></PersonalCentrality>
}

object PersonalCentrality {
  def apply(pid: Long) = new PersonalCentrality(pid, 0.0)
  def apply(pid: Long, score: Double) = new PersonalCentrality(pid, score)
  
  /** Create from XML data. */
  def fromXML(node: Node): PersonalCentrality = {
    val id = (node \ "ID").text.toLong
    val score = (node \ "Score").text.toDouble
    new PersonalCentrality(id, score)
  }
}
