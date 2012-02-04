package org.spiffy.enron

import scala.xml.Node

/** A person.
  * @constructor
  * @param pid The personal identifier (people.personid).
  * @param unified The unified personal identifier.  Same as (pid) if there is only one record for this person
  * but points to the primary ID if there are duplicates.
  * @param name Canonicalized personal name: real name or e-mail prefix derived.
  */
class Person private (val pid: Long, val unified: Long, val name: String)
  extends PersonalIdentified {
  override def toString = "Person(pid=" + pid + ", unified=" + unified + ", name=\"" + name + "\")"

  /** Convert to XML representation. */
  def toXML = <Person><ID>{ pid }</ID><UnifiedID>{ unified }</UnifiedID><Name>{ name }</Name></Person>
}

object Person {
  def apply(pid: Long, name: String) = new Person(pid, pid, name)
  def apply(pid: Long, unified: Long, name: String) = new Person(pid, unified, name)

  /** Create from XML data. */
  def fromXML(node: Node): Person = {
    val id = (node \ "ID").text.toLong
    val unified = (node \ "UnifiedID").text.toLong
    val name = (node \ "Name").text
    new Person(id, unified, name)
  }
}
