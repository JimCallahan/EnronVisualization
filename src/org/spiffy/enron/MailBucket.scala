package org.spiffy.enron

import collection.mutable.HashMap
import collection.immutable.{ Queue, SortedSet, TreeMap, TreeSet }

/** A counter of e-mails sent and received by each person at each point in time. */
class MailBucket {
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

  /** The range of times stored.
    * @return The first and last period time stamps (in UTC milliseconds).
    */
  def timeRange: (Long, Long) =
    ((Long.MaxValue, 0L) /: table.keys) {
      case ((first, last), stamp) => (first min stamp, last max stamp)
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
    val stamps = new TreeSet[Long] ++ table.keySet
    val rtn = new Array[Activity](stamps.size)
    var i = 0
    for (stamp <- stamps) {
      rtn(i) = if (!table.contains(stamp)) Activity()
      else table(stamp).getOrElse(pid, Activity())
      i = i + 1
    }
    rtn
  }

  /** Get the average e-mail activity history of a given person for each time period.
    * @param pid The unique personal identifier (people.personid).
    * @param samples The number of samples to average.
    */
  def personalAverageActivity(pid: Long, samples: Int): Array[AverageActivity] = {
    var q = Queue[Activity]()
    for (act <- personalActivity(pid)) yield {
      q = q.enqueue(act)
      while (q.size > samples) {
        val (_, nq) = q.dequeue
        q = nq
      }
      AverageActivity(q.reduce(_ + _), samples)
    }
  }
}
