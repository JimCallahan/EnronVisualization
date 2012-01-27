package org.spiffy.enron

import collection.mutable.HashMap
import collection.immutable.{ Queue, SortedSet, TreeMap, TreeSet }

class SentimentBucket(val firstStamp: Long, val lastStamp: Long, val interval: Long)
  extends TimeSampled {
  /** Internal storage for messageIDs: stamp -> senderID -> receiverID -> message IDs */
  private var messages = new HashMap[Long, HashMap[Long, HashMap[Long, TreeSet[Long]]]]

  /** Sentiment for each message referenced by "messages". */
  private var sentiment = new HashMap[Long, Sentiment]

  /** Internal storage for sentiment counters: stamp -> senderID -> receiverID -> sentiment */
  private var table = new HashMap[Long, HashMap[Long, HashMap[Long, Sentiment]]]

  /** Increment the e-mail counters and messageIDs.
    * @param stamp The time stamp of start of period (in UTC milliseconds).
    * @param sendID The unique identifier of the person who sent the e-mail (people.personid).
    * @param recvID The unique identifier of the person who received the e-mail (people.personid).
    * @param msgID The unique identifier for the message (bodies.messageID).
    */
  def inc(stamp: Long, sendID: Long, recvID: Long, msgID: Long) {
    {
      val sm = messages.getOrElseUpdate(stamp, new HashMap[Long, HashMap[Long, TreeSet[Long]]])
      val rm = sm.getOrElseUpdate(sendID, new HashMap[Long, TreeSet[Long]])
      rm += (recvID -> (rm.getOrElseUpdate(recvID, new TreeSet[Long]) + msgID))
    }

    {
      val sm = table.getOrElseUpdate(stamp, new HashMap[Long, HashMap[Long, Sentiment]])
      val rm = sm.getOrElseUpdate(sendID, new HashMap[Long, Sentiment])
      val snt = rm.getOrElseUpdate(recvID, Sentiment())
      snt.inc
    }
  }

  /** Add financial terms found in the given e-mail message to the stored sentiment. */
  def addTerms(msgID: Long, terms: Iterable[FinancialTerm.Value]) {
    val snt = sentiment.getOrElseUpdate(msgID, Sentiment())
    for (t <- terms) snt.inc(t)
  }

  /** Combine the per-message sentiment added with addTerms() to the user link counters.
    * For correct results, this should be called after all inc() and addTerms() calls, but before any other methods.
    */
  def collate() {
    for (stamp <- table.keys) {
      val sm = table(stamp)
      for (sendID <- sm.keys) {
        val rm = sm(sendID)
        for (recvID <- rm.keys) {
          val snt = rm(recvID)
          for (msgID <- messages(stamp)(sendID)(recvID))
            snt += sentiment.getOrElse(msgID, Sentiment())
        }
      }
    }

    // Reset the per-message caching tables to free memory.
    messages = new HashMap[Long, HashMap[Long, HashMap[Long, TreeSet[Long]]]]
    sentiment = new HashMap[Long, Sentiment]
  }

  /** Get the total number and sentiment of bidirectional e-mails sent between two people removing duplicates.
    * Order of sender/receiver does not matter in the results and any given pair of people will only
    * be included once.
    */
  def totalBiSentiment: TreeSet[BiSentiment] = {
    // Total e-mail counters: senderID -> receiverID -> sentiment 
    val totals = new HashMap[Long, HashMap[Long, Sentiment]]
    for ((_, sm) <- table; (sid, rm) <- sm; (rid, snt) <- rm) {
      val m = totals.getOrElseUpdate(sid, new HashMap[Long, Sentiment])
      m.getOrElseUpdate(rid, Sentiment()) += snt
    }

    // Total unique bidirectional e-mails counters: senderID -> receiverID -> sentiment
    val bidir = new HashMap[Long, HashMap[Long, BiSentiment]]
    for ((sid, rm) <- totals; (rid, srSent) <- rm) {
      if ((sid != rid) && totals.contains(rid)) {
        val sm = totals(rid)
        if (sm.contains(sid)) {
          // Skip B->A bidirectional count if A->B is already known  
          if (!bidir.contains(rid) || !bidir(rid).contains(sid)) {
            val rsSent = sm(sid)
            bidir.getOrElseUpdate(sid, new HashMap[Long, BiSentiment]) += (rid -> BiSentiment(sid, rid, srSent, rsSent))
          }
        }
      }
    }

    var rtn = new TreeSet[BiSentiment]
    for ((sid, rm) <- bidir; (rid, snt) <- rm)
      rtn += snt
    rtn
  }

  /** Get the count and sentiment of e-mails sent from one person to another each time period.
    * @param pid The unique personal identifier (people.personid).
    */
  def sentimentHistory(sendID: Long, recvID: Long): Array[Sentiment] = {
    val rtn = Array.fill(size)(Sentiment())
    for (stamp <- table.keySet) {
      val i = intervalIndex(stamp)
      if (table.contains(stamp)) {
        val sm = table(stamp)
        if (sm.contains(sendID)) {
          val rm = sm(sendID)
          if (rm.contains(recvID))
            rtn(i) = rm(recvID)
        }
      }
    }
    rtn
  }

    /** Get the average amount of e-mails sent from one person to another for each time period.
    * If the average is zero for a given time period, then the returned Traffic entry will be omitted.
    * @param window The number of samples to average.
    */
  def averageBiSentiment(window: Int): Array[TreeSet[AverageBiSentiment]] = {
    val rtn = Array.fill(size - window + 1) { new TreeSet[AverageBiSentiment]() }

    for (btr <- totalBiSentiment) {
      val srHist =
        for (ca <- sentimentHistory(btr.sendID, btr.recvID).sliding(window)) yield {
          val total = Sentiment()
          for(snt <- ca)
            total += snt
          AverageSentiment(total, window)
        }

      val rsHist =
        for (ca <- sentimentHistory(btr.recvID, btr.sendID).sliding(window)) yield {
          val total = Sentiment()
          for(snt <- ca)
            total += snt
          AverageSentiment(total, window)
        }

      var i = 0
      for ((s, r) <- (srHist zip rsHist)) {
        if ((s.words > 0.0) || (r.words > 0.0))
          rtn(i) = rtn(i) + AverageBiSentiment(btr.sendID, btr.recvID, s, r)
        i = i + 1
      }
    }

    rtn
  }
}

object SentimentBucket {
  def apply(sampled: TimeSampled) =
    new SentimentBucket(sampled.firstStamp, sampled.lastStamp, sampled.interval)
}