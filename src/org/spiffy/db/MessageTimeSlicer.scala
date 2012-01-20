package org.spiffy.db

import org.scalagfx.io.Path
import org.scalagfx.math.{Pos3d, Index3i}
import org.scalagfx.houdini.geo.GeoWriter

import collection.mutable.HashMap
import collection.immutable.{ TreeMap, TreeSet }

import java.sql.{ Connection, DriverManager, ResultSet, SQLException, Timestamp }
import java.util.{ Calendar, Date, GregorianCalendar }
import java.io.{ BufferedWriter, FileWriter }

object MessageTimeSlicer {
  
  //-----------------------------------------------------------------------------------------------------------------------------------
  //   C L A S S E S 
  //-----------------------------------------------------------------------------------------------------------------------------------
  
  /** The real name (or e-mail prefix if not known) of a person and the e-mail domain of their address. */
  class Person(val name: String, val domain: String) {
    override def toString = "Name(" + name + ")  Domain(" + domain + ")"
  }

  /**
   * Usage statistics on the number of e-mails involving a specific person.
   * @constructor
   * @param sent The number of messages sent.
   * @param received The number of message received.
   */
  class Activity(val id: Long, val sent: Long, val received: Long)
    extends Ordered[Activity] {
    def compare(that: Activity): Int = total compare that.total

    /** The total number e-mails sent and received by the person. */
    def total = sent + received

    override def toString = id + "(" + sent + ":" + received + ")"
  }

  /**
   * Usage statistics for all people sending e-mails from a given e-mail domain.
   * @constructor
   * @param domain The name of the domain portion of the e-mail address.
   * @param activity The e-mail statistics for each person active during the period indexed by unique ID of person (people.personid).
   * @param others The unique IDs (people.personid) of all people grouped into the other (ID=0) activity category.
   */
  class ActiveDomain(val domain: String, val activity: TreeMap[Long, Activity], others: TreeSet[Long])
    extends Ordered[ActiveDomain] {
    def compare(that: ActiveDomain): Int = total compare that.total

    /** Is the person (identified by unique ID) included in this domain. */
    def isPerson(id: Long): Boolean = activity.contains(id) || others.contains(id)

    /** The total number of people included in this domain. */
    val people = activity.size + others.size

    /** The total number e-mails sent and received from this domain. */
    val total = activity.values.map(_.total).reduce(_ + _)

    private val offsets = {
       var rtn = new TreeMap[Long, Long]
        var cnt = 0L
        for(a <- (new TreeSet[Activity]) ++ activity.values) {
          rtn = rtn + (a.id -> cnt)
          cnt = cnt +  a.total
        }
        rtn
      }

    /** The total number of e-mails before the given person when sorted. */
    def offset(id: Long): Long = offsets(id) 
    
    override def toString = {
      domain + " -- People: " + activity.size + "  Others: " + others.size + "  E-Mails: " + total
    }
  }

  /**
   * Usage statistics for all people sending e-mails in a given period.
   * @constructor
   * @param stamp Time stamp of start of period (in UTC milliseconds).
   * @param domains Activity from each e-mail address domain.
   */
  class ActivePeriod(val stamp: Long, val domains: TreeSet[ActiveDomain])
    extends Ordered[ActivePeriod] {
    def compare(that: ActivePeriod): Int = stamp compare that.stamp

    /** Is the person (identified by unique ID) included in these activity statistics. */
    def isPerson(id: Long): Boolean = domains.exists(_.isPerson(id))

    /** The total number of people included in the period. */
    val people = domains.map(_.people).reduce(_ + _)

    /** The total number e-mails sent and received during the period. */
    val total = domains.map(_.total).reduce(_ + _)

    /** The total number of e-mails before the given domain. */
    def offset(dname: String): Long = {
      var cnt = 0L
      var rtn = 0L
      for(d <- domains) {
        if(d.domain == dname) 
          rtn = cnt
        cnt = cnt + d.total
      }
      rtn
    }
    
    override def toString = {
      val cal = new GregorianCalendar
      cal.setTimeInMillis(stamp)
      cal.getTime + " -- Domains: " + domains.size + "  Total People: " + people + "  E-Mails: " + total
    }
  }

  /** A counter of e-mails for a given time period and unique person ID. */
  class MailBucket
    extends HashMap[Long, HashMap[Long, Long]] {
    /**
     * Increment the e-mail counter.
     * @param stamp The time stamp of start of period (in UTC milliseconds).
     * @param id The unique identifier of this person (people.personid).
     */
    def inc(stamp: Long, id: Long) {
      val m = getOrElseUpdate(stamp, new HashMap[Long, Long])
      m += id -> (m.getOrElseUpdate(id, 0L) + 1L)
    }

    /**
     * Get the count of e-mails.
     * @param stamp The time stamp of start of period (in UTC milliseconds).
     * @param id The unique identifier of this person (people.personid).
     */
    def count(stamp: Long, id: Long): Long =
      if (contains(stamp)) this(stamp).getOrElse(id, 0L) else 0L
  }

  
  //-----------------------------------------------------------------------------------------------------------------------------------
  //   M A I N
  //-----------------------------------------------------------------------------------------------------------------------------------

  // Loads the JDBC driver. 
  classOf[com.mysql.jdbc.Driver]
  
  /** Top level method. */
  def main(args: Array[String]) {
    val outdir = Path("./artwork/houdini/geo")
    
    try {
      val cal = new GregorianCalendar
      val connStr = "jdbc:mysql://localhost:3306/enron?user=enron&password=slimyfucks"
      val conn = DriverManager.getConnection(connStr)
      try {
        print("Determining the Active Interval: ")
        val range @ (firstMonth, lastMonth) = {
          val threshold = 10000
          val perMonth = activeMonths(conn, threshold)

          // Debug
          for ((ms, cnt) <- perMonth) {
            cal.setTimeInMillis(ms)
            print((cal.get(Calendar.MONTH) + 1) + "/" + cal.get(Calendar.YEAR) + "=" + cnt + " ")
          }
          // Debug

          (perMonth.firstKey, perMonth.lastKey)
        }

        println
        println("Collecting People: ")
        val people = {
          val ps = collectPeople(conn)

          // Debug
          for ((pid, p) <- ps)
            println("  " + pid + " = " + p)
          // Debug

          ps
        }

        println
        println("Gathering Per-User Stats for each Week...")
        val periods = {
          val threshold = 2
          val percent = 0.005
          val interval = 14 * 24 * 60 * 60 * 1000 // 2-weeks
          val rtn = activePeriods(conn, people, range, interval, threshold, percent)

          // Debug
          for (p <- rtn) {
            println
            println(p)
            for (d <- p.domains) {
              println("  " + d)
              print("   ")
              for ((id, a) <- d.activity)
                print(" " + id + ":" + a + "[%.6f]".format(a.total.toDouble / p.total.toDouble))
              println
            }
          }
          // Debug

          rtn
        }

        println
        println("Generating Per-User Send/Receive Geometry...")
        generateUserGeo(outdir, periods.first, 0)
        
        
        // ...

      } finally {
        conn.close
      }
    } catch {
      case ex =>
        println("Uncaught Exception: " + ex.getMessage + "\n" +
          "Stack Trace:\n" + ex.getStackTraceString)
    }
  }

  
  //-----------------------------------------------------------------------------------------------------------------------------------
  //   D A T A B A S E 
  //-----------------------------------------------------------------------------------------------------------------------------------

  /**
   * Get the number of e-mails received per month (index in UTC milliseconds), ignoring those months with
   * less activity than the given threshold number of emails.
   * @param conn The SQL connection.
   * @param threshold The minimum amount of e-mail activity.
   */
  def activeMonths(conn: Connection, threshold: Long): TreeMap[Long, Long] = {
    val cal = new GregorianCalendar
    var perMonth = new HashMap[Long, Long]
    val st = conn.createStatement
    val rs = st.executeQuery(
      "SELECT messagedt FROM recipients, messages " +
        "WHERE recipients.messageid = messages.messageid")
    while (rs.next) {
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
      } catch {
        case _: SQLException => // Ignore invalid time stamps.
      }
    }

    (new TreeMap[Long, Long] /: perMonth)((rtn, e) => rtn + e)
      .filter(_ match { case (_, cnt) => cnt > threshold })
  }

  /**
   * Lookup the names of all the users and split their e-mail addresses into name/domain.
   * Discards users without valid names, domains or which have bogus names.
   * @param conn The SQL connection.
   */
  def collectPeople(conn: Connection): TreeMap[Long, Person] = {
    var rtn = new TreeMap[Long, Person]

    val st = conn.createStatement
    val rs = st.executeQuery("SELECT personid, email, name FROM people")
    while (rs.next) {
      try {
        val pid = rs.getInt(1)
        val addr = rs.getString(2)
        val nm = rs.getString(3)

        val (prefix, domain) =
          if (addr == null) ("unknown", "unknown")
          else {
            addr.filter(_ != '"').split("@") match {
              case Array(p, d) => (p, d)
              case _ => (addr, "unknown")
            }
          }

        val name =
          if (nm != null) {
            val n = nm.filter(_ != '"')
            if (n.size > 0) n else prefix
          } else prefix

        // Toss out bogus e-mails.
        (name, domain) match {
          case ("e-mail", "enron.com") =>
          case ("unknown", _) =>
          case (_, "unknown") =>
          case _ => rtn = rtn + (pid.toLong -> new Person(name, domain))
        }
      } catch {
        case _: SQLException => // Ignore invalid people.
      }
    }

    rtn
  }

  /**
   * Collates per-user send/receive statistics.
   * Discards users which have sent fewer than (threshold) emails and groups all user which contribute less
   * then (percent) e-mails into an other category and creates a ID=0 user to represent them.
   * @param conn The SQL connection.
   * @param people The name/domain information for each person indexed by unique ID.
   * @param interval The number of milliseconds in each time period.
   * @param threshold The minimum number of sent e-mails needed for inclusion.
   * @param percent The minimum portion (0.0-1.0) of total e-mail activity needed for individual status.
   */
  def activePeriods(conn: Connection,
                    people: TreeMap[Long, Person],
                    range: (Long, Long),
                    interval: Long,
                    threshold: Long,
                    percent: Double): TreeSet[ActivePeriod] = {
    
    val cal = new GregorianCalendar
    val (first, last) = range

    val sent = new MailBucket
    val recv = new MailBucket

    // Get the per-user sent/receive counts for each time interval.
    {
      val st = conn.createStatement
      val rs = st.executeQuery(
        "SELECT messagedt, senderid, personid FROM recipients, messages " +
          "WHERE recipients.messageid = messages.messageid")
      while (rs.next) {
        try {
          val ts = rs.getTimestamp(1)
          val sid = rs.getInt(2)
          val rid = rs.getInt(3)

          cal.setTime(ts)
          val ms = cal.getTimeInMillis

          if ((first <= ms) && (ms <= last)) {
            val head = ms - ((ms - first) % interval)
            sent.inc(head, sid)
            recv.inc(head, rid)
          }
        } catch {
          case _: SQLException => // Ignore invalid time stamps.
        }
      }
    }

    var periods = new TreeSet[ActivePeriod]
    for (ms <- new TreeSet[Long]() ++ sent.keySet) { // sent and recv must have same keys 
      var act = new TreeMap[Long, Activity]
      for (id <- new TreeSet[Long]() ++ sent(ms).keySet ++ recv(ms).keySet) {
        val s = sent.count(ms, id)
        val r = recv.count(ms, id)
        if (s > threshold)
          act = act + (id -> new Activity(id, s, r))
      }

      val byDomain = act.filter { case (id, _) => people.contains(id) }.groupBy { case (id, _) => people(id).domain }
      val total = byDomain.map { case (_, dact) => dact.values.map(_.total).reduce(_ + _) }.reduce(_ + _).toDouble

      var domains = new TreeSet[ActiveDomain]
      for ((dname, dact) <- byDomain) {
        val (important, others) = dact.partition { case (id, a) => (a.total.toDouble / total) > percent }
        val (osent, orecv) = ((0L, 0L) /: others) { case ((s, r), (_, a)) => (s + a.sent, r + a.received) }
        if (!important.isEmpty && !others.isEmpty) {
          domains = domains +
            new ActiveDomain(dname, important + (0L -> new Activity(0L, osent, orecv)), (new TreeSet[Long]) ++ others.keySet)
        }
      }

      periods = periods + new ActivePeriod(ms, domains)
    }

    periods
  }
  

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   G E O M E T R Y
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** */
  def generateUserGeo(outdir: Path, period: ActivePeriod, frame: Int) {
    val path = outdir + ("users.%04d.geo".format(frame))
    println("Writing: " + path)
	val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val pts = List(Pos3d(-1.0, -1.0, 0.0), Pos3d(1.0, -1.0, 0.0), Pos3d(1.0, 1.0, 0.0), Pos3d(-1.0, 1.0, 0.0)) 
      val idxs = List(Index3i(0, 2, 1), Index3i(0, 3, 2))
      
      val geo = GeoWriter(pts.size, idxs.size)
      geo.writeHeader(out)
      
      for(p <- pts) geo.writePoint(out, p)
      
      geo.writePolygon(out, idxs.size)
      for(i <- idxs) geo.writeTriangle(out, i)
      
      geo.writeFooter(out)
    }
    finally {
      out.close
    }
  }
  
}