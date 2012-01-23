package org.spiffy.enron

/** The e-mail activity of a person.
  * @constructor
  * @param pid The unique identifier (people.personid).
  * @param sent The number of messages sent.
  * @param recv The number of message received.
  */
class PersonalActivity private (val pid: Long, sent: Long, recv: Long)
  extends Activity(sent, recv)
  with PersonalIdentified
  with Ordered[PersonalActivity] {
  /** Ordered in descending total number of e-mails and ascending IDs. */
  def compare(that: PersonalActivity): Int =
    (that.total compare total) match {
      case 0 => pid compare that.pid
      case c => c
    }

  override def toString = "PersonalActivity(pid=" + pid + ", sent=" + sent + ", recv=" + recv + ")"
}

object PersonalActivity {
  def apply(pid: Long) = new PersonalActivity(pid, 0L, 0L)
  def apply(pid: Long, act: Activity) = new PersonalActivity(pid, act.sent, act.recv)
  def apply(pid: Long, sent: Long, recv: Long) = new PersonalActivity(pid, sent, recv)
}
