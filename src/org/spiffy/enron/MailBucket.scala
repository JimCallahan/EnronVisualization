package org.spiffy.enron

import collection.mutable.HashMap
import collection.immutable.{ Queue, SortedSet, TreeMap, TreeSet }

/** A counter of e-mails sent and received by each person at each point in time. */
class MailBucket(val firstStamp: Long, val lastStamp: Long, val interval: Long)
  extends TimeSampled {
  /** Internal storage for counters: stamp -> pid -> activity */
  private var table = new HashMap[Long, HashMap[Long, Activity]]

  /** Increment the e-mail counter.
    * @param stamp The time stamp of start of period (in UTC milliseconds).
    * @param sendID The unique identifier of the person who sent the e-mail (people.personid).
    * @param recvID The unique identifier of the person who received the e-mail (people.personid).
    */
  def inc(stamp: Long, sendID: Long, recvID: Long) {
    val m = table.getOrElseUpdate(stamp, new HashMap[Long, Activity])
    m += (sendID -> m.getOrElseUpdate(sendID, Activity()).incSend)
    m += (recvID -> m.getOrElseUpdate(sendID, Activity()).incRecv)
  }

  /** The time period sample time stamps. */
  def sampledPeriods: TreeSet[Long] =
    (new TreeSet[Long]) ++ table.keySet

  /** The total number of e-mails during the given period.
    * @param stamp The time stamp of start of period (in UTC milliseconds).
    */
  def totalPeriodActivity(stamp: Long): Activity = {
    if (!table.contains(stamp)) Activity()
    else (Activity() /: table(stamp).values)(_ + _)
  }

  /** Get the total e-mail activity of each person sorted by most to least active. */
  def totalPersonalActivity: TreeSet[PersonalActivity] = {
    var totals = new HashMap[Long, Activity]
    for (m <- table.values) {
      for ((pid, act) <- m) {
        totals += (pid -> (totals.getOrElseUpdate(pid, Activity()) + act))
      }
    }
    ((new TreeSet[PersonalActivity]) /: totals) {
      case (rtn, (pid, act)) => rtn + PersonalActivity(pid, act)
    }
  }

  /** Get the e-mail activity history of a given person for each time period.
    * @param pid The unique personal identifier (people.personid).
    */
  def personalActivity(pid: Long): Array[Activity] = {
    val rtn = Array.fill(size)(Activity())
    for (stamp <- table.keySet) {
      val i = intervalIndex(stamp)
      rtn(i) = if (!table.contains(stamp)) Activity()
      else table(stamp).getOrElse(pid, Activity())
    }
    rtn
  }

  /** Get the average e-mail activity history of a given person for each time period.
    * @param pid The unique personal identifier (people.personid).
    * @param window The number of samples to average.
    */
  def personalAverageActivity(pid: Long, window: Int): Array[AverageActivity] = {
    val rtn = for (as <- personalActivity(pid).sliding(window)) yield { 
      AverageActivity(as.reduce(_ + _), window)
    }
    rtn.toArray
  }
}

object MailBucket {
  def apply(sampled: TimeSampled) =
    new MailBucket(sampled.firstStamp, sampled.lastStamp, sampled.interval)
}
