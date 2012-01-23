package org.spiffy.enron

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
}

object Person {
  def apply(pid: Long, name: String) = new Person(pid, pid, name)
  def apply(pid: Long, unified: Long, name: String) = new Person(pid, unified, name)
}
