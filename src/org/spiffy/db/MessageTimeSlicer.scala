package org.spiffy.db

import collection.mutable.HashMap
import collection.immutable.{TreeMap, TreeSet}

import java.sql.{Connection, DriverManager, ResultSet, SQLException, Timestamp}
import java.util.{Calendar, Date, GregorianCalendar}

object MessageTimeSlicer 
{
  /** Usage statistics on the number of e-mails involving a specific person. 
    * @constructor
    * @param sent The number of messages sent.
    * @param received The number of message received. */
  class Activity(val sent: Long, val received: Long) 
  {
    /** The total number e-mails sent and received by the person. */
    def total = sent + received
    
    override def toString = "(" + sent + ":" + received + ")"  
  }
  
  /** Usage statistics for all people sending e-mails in a given period.
   * @constructor
   * @param stamp Time stamp of start of period (in UTC milliseconds).
   * @param activity The e-mail statistics for each person active during the period indexed by unique ID of person (people.personid). 
   * @param others The unique IDs (people.personid) of all people grouped into the other (ID=0) activity category. */
  class ActivePeriod(val stamp: Long, val activity: TreeMap[Long,Activity], others: TreeSet[Long])
    extends Ordered[ActivePeriod]
  {
    def compare(that: ActivePeriod): Int = stamp compare that.stamp
    
    /** Is the person (identified by unique ID) included in these activity statistics. */
    def isPerson(id: Long): Boolean = activity.contains(id) || others.contains(id) 
   
    /** The total number of people included in the period. */
    val people = activity.size + others.size
    
    /** The total number e-mails sent and received during the period. */
    val total = activity.values.map(_.total).reduce(_ + _)
    
    override def toString = {
      val cal = new GregorianCalendar
      cal.setTimeInMillis(stamp)
      cal.getTime + " -- People: " + activity.size + "  Others: " + others.size + "  E-Mails: " + total  
    }
  }
  
  /** A counter of e-mails for a given time period and person. */
  class MailBucket 
    extends HashMap[Long, HashMap[Long,Long]]
  {
    /** Increment the e-mail counter.
      * @param stamp The time stamp of start of period (in UTC milliseconds).
      * @param id The unique identifier of this person (people.personid). */
    def inc(stamp: Long, id: Long) {
      val m = getOrElseUpdate(stamp, new HashMap[Long,Long])
      m += id -> (m.getOrElseUpdate(id, 0L) + 1L)
    }
     
    /** Get the count of e-mails.
      * @param stamp The time stamp of start of period (in UTC milliseconds).
      * @param id The unique identifier of this person (people.personid). */
    def count(stamp: Long, id: Long): Long = 
      if(contains(stamp)) this(stamp).getOrElse(id, 0L) else 0L
  }
  
  // Loads the JDBC driver. 
  classOf[com.mysql.jdbc.Driver]
 
  /** Top level method. */
  def main(args: Array[String]) {
    try {
      val cal = new GregorianCalendar
      val connStr = "jdbc:mysql://localhost:3306/enron?user=enron&password=slimyfucks"
      val conn = DriverManager.getConnection(connStr)
      try {
        print("Determining the Active Interval: ")
        val range @ (firstMonth, lastMonth) = {
          val threshold = 10000
    	  val perMonth = activeMonths(conn, threshold)
          for((ms, cnt) <- perMonth) {
            cal.setTimeInMillis(ms)
	        print((cal.get(Calendar.MONTH) + 1) + "/" + cal.get(Calendar.YEAR) + "=" + cnt + " ")
    	  }
    	  (perMonth.firstKey, perMonth.lastKey)
        }
        
        println        
        println("Gathering Per-User Stats for each Week: ")
        val stats = {
          val threshold = 2
          val percent = 0.005
          val interval = 14*24*60*60*1000 // 2-weeks
          val s = activity(conn, range, interval, threshold, percent) 
          for(w <- s) {
            println(w)
            for((id, a) <- w.activity)
              print(" " + id + ":" + a + "[%.6f]".format(a.total.toDouble/w.total.toDouble))
            println
          }
          s
        }
        
        
        // ...
        
      } 
      finally {
        conn.close
      }
    } 
    catch {
      case ex =>
        println("Uncaught Exception: " + ex.getMessage + "\n" +
          "Stack Trace:\n" + ex.getStackTraceString)
    }
  }
  
  /** Get the number of e-mails received per month (index in UTC milliseconds), ignoring those months with 
    * less activity than the given threshold number of emails. 
    * @param conn The SQL connection.
    * @param threshold The minimum amount of e-mail activity. */
  def activeMonths(conn: Connection, threshold: Long): TreeMap[Long,Long] = {
    val cal = new GregorianCalendar
	var perMonth = new HashMap[Long,Long]
	val st = conn.createStatement
    val rs = st.executeQuery(
      "SELECT messagedt FROM recipients, messages " + 
      "WHERE recipients.messageid = messages.messageid")
    while(rs.next) {
      try {
        val ts = rs.getTimestamp(1)
        
        cal.setTime(ts)
        cal.set(Calendar.DAY_OF_MONTH, 1)
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        val ms = cal.getTimeInMillis
        
        perMonth += ms -> (perMonth.getOrElse(ms, 0L) + 1L)        
      }
      catch {
        case _: SQLException => // Ignore invalid time stamps.
      }
    }

    (new TreeMap[Long,Long] /: perMonth)((rtn, e) => rtn + e)
      .filter(_ match { case (_, cnt) => cnt > threshold })
  }
  
  /** Collates per-user send/receive statistics. 
    * Discards users which have sent fewer than (threshold) emails and groups all user which contribute less
    * then (percent) emails into an other category and creates a ID=0 user to represent them.
    * @param conn The SQL connection.
    * @param interval The number of milliseconds in each time period.
    * @param threshold The minimum number of sent e-mails needed for inclusion.
    * @param percent The minimum portion (0.0-1.0) of total e-mail activity needed for individual status. */
  def activity(conn: Connection, range: (Long, Long), interval: Long, threshold:  Long, percent: Double): TreeSet[ActivePeriod] = {
    val cal = new GregorianCalendar
	val (first, last) = range
	
    val sent = new MailBucket
    val recv = new MailBucket
    
	val st = conn.createStatement
    val rs = st.executeQuery(
      "SELECT messagedt, senderid, personid FROM recipients, messages " + 
      "WHERE recipients.messageid = messages.messageid")
    while(rs.next) {
      try {
        val ts = rs.getTimestamp(1)
        val sid = rs.getInt(2)
        val rid = rs.getInt(3)
        
        cal.setTime(ts)
        val ms = cal.getTimeInMillis
        
        if((first <= ms) && (ms <= last)) {
          val head = ms - ((ms - first) % interval)
          sent.inc(head, sid)
          recv.inc(head, rid)
        }
      }
      catch {
        case _: SQLException => // Ignore invalid time stamps.
      }
    }

    var weeks = new TreeSet[ActivePeriod]
    for(ms <- new TreeSet[Long]() ++ sent.keySet) {  // sent and recv must have same keys 
      var act = new TreeMap[Long,Activity] 
      for(id <- new TreeSet[Long]() ++ sent(ms).keySet ++ recv(ms).keySet) {
    	val s = sent.count(ms, id)
    	val r = recv.count(ms, id)
    	if(s > threshold)
    	  act = act + (id -> new Activity(s, r)) 
      }
      
      val all = new ActivePeriod(ms, act, new TreeSet)
      val (important, others) = act.partition{ case(id, a) => (a.total.toDouble / all.total.toDouble) > percent } 
      val (osent, orecv) = ((0L, 0L) /: others){ case ((s, r), (_, a)) => (s+a.sent, r+a.received) }
      
      weeks = weeks + new ActivePeriod(ms, important + (0L -> new Activity(osent, orecv)), (new TreeSet[Long]) ++ others.keySet)
    }
    
    weeks
  }
}