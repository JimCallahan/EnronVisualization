package org.spiffy.enron

import collection.mutable.HashMap
import collection.immutable.{ Queue, SortedSet, TreeMap, TreeSet }

class TrafficBucket(val firstStamp: Long, val lastStamp: Long, val interval: Long)
  extends TimeSampled {
  /** Internal storage for counters: stamp -> senderID -> receiverID -> count */
  private var table = new HashMap[Long, HashMap[Long, HashMap[Long, Long]]]

  /** Increment the e-mail counter.
    * @param stamp The time stamp of start of period (in UTC milliseconds).
    * @param sendID The unique identifier of the person who sent the e-mail (people.personid).
    * @param recvID The unique identifier of the person who received the e-mail (people.personid).
    */
  def inc(stamp: Long, sendID: Long, recvID: Long) {
    val sm = table.getOrElseUpdate(stamp, new HashMap[Long, HashMap[Long, Long]])
    val rm = sm.getOrElseUpdate(sendID, new HashMap[Long, Long])
    rm += (recvID -> (rm.getOrElseUpdate(recvID, 0L) + 1L))
  }

  /** Get the total number of bidirectional e-mails sent between two people removing duplicates.
    * Order of sender/receiver does not matter in the results and any given pair of people will only
    * be included once.
    */
  def totalBiTraffic: TreeSet[BiTraffic] = {
    // Total e-mail counters: senderID -> receiverID -> count 
    val totals = new HashMap[Long, HashMap[Long, Long]]
    for ((_, sm) <- table; (sid, rm) <- sm; (rid, count) <- rm) {
      val m = totals.getOrElseUpdate(sid, new HashMap[Long, Long])
      m += (rid -> (m.getOrElseUpdate(rid, 0L) + count))
    }

    // Total unique bidirectional e-mails counters: senderID -> receiverID -> traffic
    val bidir = new HashMap[Long, HashMap[Long, BiTraffic]]
    for ((sid, rm) <- totals; (rid, srCount) <- rm) {
      if ((sid != rid) && totals.contains(rid)) {
        val sm = totals(rid)
        if (sm.contains(sid)) {
          // Skip B->A bidirectional count if A->B is already known  
          if (!bidir.contains(rid) || !bidir(rid).contains(sid)) {
            val rsCount = sm(sid)
            bidir.getOrElseUpdate(sid, new HashMap[Long, BiTraffic]) += (rid -> BiTraffic(sid, rid, srCount, rsCount))
          }
        }
      }
    }

    var rtn = new TreeSet[BiTraffic]
    for ((sid, rm) <- bidir; (rid, tr) <- rm)
      rtn += tr
    rtn
  }

  /** Get the count of e-mails sent from one person to another each time period.
    * @param pid The unique personal identifier (people.personid).
    */
  def trafficHistory(sendID: Long, recvID: Long): Array[Long] = {
    val rtn = Array.fill(size)(0L)
    for (stamp <- table.keySet) {
      val i = intervalIndex(stamp)
      rtn(i) =
        if (!table.contains(stamp)) 0L
        else {
          val sm = table(stamp)
          if (!sm.contains(sendID)) 0L
          else {
            val rm = sm(sendID)
            if (!rm.contains(recvID)) 0L
            else rm(recvID)
          }
        }
    }
    rtn
  }

  /** Get the average amount of e-mails sent from one person to another for each time period.
    * If the average is zero for a given time period, then the returned Traffic entry will be omitted.
    * @param window The number of samples to average.
    */
  def averageBiTraffic(window: Int): Array[TreeSet[AverageBiTraffic]] = {
    val rtn = Array.fill(size - window + 1) { new TreeSet[AverageBiTraffic]() }

    for (btr <- totalBiTraffic) {
      val srHist =
        for (ca <- trafficHistory(btr.sendID, btr.recvID).sliding(window)) yield {
          ca.map(_.toDouble).reduce(_ + _) / window.toDouble
        }

      val rsHist =
        for (ca <- trafficHistory(btr.recvID, btr.sendID).sliding(window)) yield {
          ca.map(_.toDouble).reduce(_ + _) / window.toDouble
        }

      var i = 0
      for ((s, r) <- (srHist zip rsHist)) {
        if ((s > 0.0) || (r > 0.0))
          rtn(i) = rtn(i) + AverageBiTraffic(btr.sendID, btr.recvID, s, r)
        i = i + 1
      }
    }

    rtn
  }
}

object TrafficBucket {
  def apply(sampled: TimeSampled) =
    new TrafficBucket(sampled.firstStamp, sampled.lastStamp, sampled.interval)
}