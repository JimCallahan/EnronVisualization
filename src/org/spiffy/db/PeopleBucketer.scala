package org.spiffy.db

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Pos3d, Index3i, Frame2d, Scalar }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PrimitiveIntAttr }

import collection.mutable.HashMap
import collection.immutable.{ Queue, TreeMap, TreeSet }

import java.sql.{ Connection, DriverManager, ResultSet, SQLException, Timestamp }
import java.util.{ Calendar, Date, GregorianCalendar }
import java.io.{ BufferedWriter, FileWriter }

object PeopleBucketer {

  //---------------------------------------------------------------------------------------------------------
  //   C L A S S E S 
  //---------------------------------------------------------------------------------------------------------

  /** A counter of e-mails sent and received by each person at each point in time. */
  class MailBucket {
    /** Internal storage for counters: stamp -> pid -> activity */
    private var table = new HashMap[Long, HashMap[Long, Activity]]

    /**
     * Increment the e-mail counter.
     * @param stamp The time stamp of start of period (in UTC milliseconds).
     * @param sendID The unique identifier of the person who sent the e-mail (people.personid).
     * @param recvID The unique identifier of the person who received the e-mail (people.personid).
     */
    def inc(stamp: Long, sendID: Long, recvID: Long) {
      val m = table.getOrElseUpdate(stamp, new HashMap[Long, Activity])
      m += (sendID -> m.getOrElseUpdate(sendID, Activity()).incSend)
      m += (recvID -> m.getOrElseUpdate(sendID, Activity()).incRecv)
    }

    /**
     * The range of times stored.
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

    /** Get the e-mail activity history of a given person for each time period. */
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
      * @param samples The number of samples to average. */
    def personalAverageActivity(pid: Long, samples: Int): Array[AverageActivity] = {
      var q = Queue[Activity]()
      for(act <- personalActivity(pid)) yield {
        q = q.enqueue(act)
        while(q.size > samples) {
          val (_, nq) = q.dequeue
          q = nq
        }
        AverageActivity(q.reduce(_ + _), samples)
      }
    }
  }

  /**
   * Some e-mail activity.
   * @constructor
   * @param sent The number of messages sent.
   * @param recv The number of message received.
   */
  class Activity protected (val sent: Long, val recv: Long) {
    /** The total number e-mails sent and received by the person. */
    def total = sent + recv

    /** Accumulate message counts. */
    def +(that: Activity) = Activity(sent + that.sent, recv + that.recv)

    /** Increment to the sent count. */
    def incSend = Activity(sent + 1, recv)

    /** Increment to the received count. */
    def incRecv = Activity(sent, recv + 1)

    override def toString = "Activity(sent=" + sent + ", recv=" + recv + ")"
  }

  object Activity {
    def apply() = new Activity(0L, 0L)
    def apply(sent: Long, recv: Long) = new Activity(sent, recv)
  }

  /**
   * The e-mail activity of a person.
   * @constructor
   * @param pid The unique identifier (people.personid).
   * @param sent The number of messages sent.
   * @param recv The number of message received.
   */
  class PersonalActivity private (val pid: Long, sent: Long, recv: Long)
    extends Activity(sent, recv)
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

  /**
   * An average of e-mail activity over an time interval.
   * @constructor
   * @param sent The average number of messages sent.
   * @param recv The average number of message received.
   */
  class AverageActivity private (val sent: Double, val recv: Double) {
    /** The average total number e-mails sent and received by the person. */
    def total = sent + recv

    override def toString = "Activity(sent=%.4f, recv=%.4f)".format(sent, recv)
  }

  object AverageActivity {
    def apply() = new AverageActivity(0.0, 0.0)
    def apply(act: Activity, samples: Long) = 
      new AverageActivity(act.sent.toDouble/samples.toDouble, act.recv.toDouble/samples.toDouble)
  }

  /**
   * A person.
   * @constructor
   * @param pid The personal identifier (people.personid).
   * @param unified The unified personal identifier.  Same as (pid) if there is only one record for this person
   * but points to the primary ID if there are duplicates.
   * @param name Canonicalized personal name: real name or e-mail prefix derived.
   */
  class Person private (val pid: Long, val unified: Long, val name: String) {
    override def toString = "Person(pid=" + pid + ", unified=" + unified + ", name=\"" + name + "\")"
  }

  object Person {
    def apply(pid: Long, name: String) = new Person(pid, pid, name)
    def apply(pid: Long, unified: Long, name: String) = new Person(pid, unified, name)
  }

  //---------------------------------------------------------------------------------------------------------
  //   M A I N    
  //---------------------------------------------------------------------------------------------------------

  // Loads the JDBC driver. 
  classOf[com.mysql.jdbc.Driver]

  /** Top level method. */
  def main(args: Array[String]) {
    try {
      val cal = new GregorianCalendar
      val connStr = "jdbc:mysql://localhost:3306/enron?user=enron&password=slimyfucks"
      val conn = DriverManager.getConnection(connStr)
      try {
        println("Determining the Active Interval...")
        val range @ (firstMonth, lastMonth) = {
          val threshold = 10000
          val perMonth = activeMonths(conn, threshold)

          val path = Path("./data/stats/monthlyTotals.csv")
          println("  Writing: " + path)
          val out = new BufferedWriter(new FileWriter(path.toFile))
          try {
            out.write("TIME STAMP,TOTAL E-MAILS,\n")
            for ((stamp, cnt) <- perMonth)
              out.write(stamp + "," + cnt + ",\n")
          } finally {
            out.close
          }

          (perMonth.firstKey, perMonth.lastKey)
        }

        //---------------------------------------------------------------------------------------------------

        println
        println("Collecting People...")
        val people = {
          val ps = collectPeople(conn)

          val path = Path("./data/stats/people.csv")
          println("  Writing: " + path)
          val out = new BufferedWriter(new FileWriter(path.toFile))
          try {
            out.write("PERSON ID,UNIFIED ID,NAME,\n")
            for ((_, p) <- ps)
              out.write(p.pid + "," + p.unified + "," + p.name + ",\n")
          } finally {
            out.close
          }

          ps
        }

        //---------------------------------------------------------------------------------------------------

        println
        println("Collect Daily Activity...")
        val bucket = {
          val interval = 24 * 60 * 60 * 1000 // 24-hours
          collectMail(conn, people, range, interval)
        }

        {
          val samples = bucket.sampledPeriods
        
          val path = Path("./data/stats/dailyActivity.csv")
          println("  Writing: " + path)
          val out = new BufferedWriter(new FileWriter(path.toFile))
          try {
            out.write("TIME STAMP,TOTAL E-MAILS,SENT E-MAILS,RECEIVED E-MAILS,\n")
            for (stamp <- samples) {
              val act = bucket.totalPeriodActivity(stamp)
              out.write(stamp + "," + act.total + "," + act.sent + "," + act.recv + ",\n")
            }
          } finally {
            out.close
          }
        }

        //---------------------------------------------------------------------------------------------------

        println
        println("Compute Personal Totals...")

        val numPeople = 100

        val personal = {
          val tpa = bucket.totalPersonalActivity

          val path = Path("./data/stats/personalActivity.csv")
          println("  Writing: " + path)
          val out = new BufferedWriter(new FileWriter(path.toFile))
          try {
            out.write("PERSON ID,TOTAL E-MAILS,SENT E-MAILS,RECEIVED E-MAILS,NAME,\n")
            for (pa <- tpa)
              out.write(pa.pid + "," + pa.total + "," + pa.sent + "," + pa.recv + "," + people(pa.pid).name + ",\n")
          } finally {
            out.close
          }
          
          tpa.take(numPeople)
        }

        //---------------------------------------------------------------------------------------------------

        println
        println("Extract Average Activity of Most Active People...")

        val avgActivity = {
          val samples = 30
          
          var aa = new TreeMap[Long, Array[AverageActivity]]
          
          for (pa <- personal)
        	aa = aa + (pa.pid -> bucket.personalAverageActivity(pa.pid, samples))

          val path = Path("./data/stats/mostActiveAveragePeople.csv")
          println("  Writing: " + path)
          val out = new BufferedWriter(new FileWriter(path.toFile))
          try {
            val (_, first) = aa.first
            for(i <- 0 until first.size) {
              for (ary <- aa.values) 
                out.write("%.8f,".format(ary(i).total))
              out.write("\n")
            }
          } finally {
            out.close
          }
          
          aa
        }

        
        //---------------------------------------------------------------------------------------------------

        println
        println("Generating Personal Activity Geometry...")

        if(true) {
          for(frame <- 0 until 787)
           generatePersonalActivityGeo(Path("./artwork/houdini/geo"), frame, personal, avgActivity)
        }
        
        //---------------------------------------------------------------------------------------------------

        println
        println("Generating Personal Label HScript...")

        generatePersonalLabelHScript(Path("./artwork/houdini/hscript"), "PeopleLabels", personal, people)
        
        
        //---------------------------------------------------------------------------------------------------
        
        println
        println("ALL DONE!")

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
   * Lookup the names of all the users, discarding those without valid names.
   * @param conn The SQL connection.
   */
  def collectPeople(conn: Connection): TreeMap[Long, Person] = {
    var rtn = new TreeMap[Long, Person]

    val nameToID = new HashMap[String, Long]

    val st = conn.createStatement
    val rs = st.executeQuery("SELECT personid, email, name FROM people")
    while (rs.next) {
      try {
        val pid = rs.getInt(1).toLong
        val addr = rs.getString(2)
        val nm = rs.getString(3)

        val (prefix, domain) =
          if (addr == null) ("unknown", "unknown")
          else {
            addr.filter(_ != '"').filter(_ != ''').split("@") match {
              case Array(p, d) => (p, d)
              case _ => (addr, "unknown")
            }
          }

        val name =
          if (nm != null) {
            val n = nm.filter(_ != '"')
            if (n.size > 0) n else prefix
          } else prefix

        // Toss out bogus addresses.
        (name, domain) match {
          case ("e-mail", "enron.com") =>
          case ("unknown", _) | (_, "unknown") =>
          case _ => {
            val canon = 
              name.toUpperCase
                .replace('.', ' ')
                .replace(',', ' ')
                .replaceAll("  ", " ")
                .trim
            if (canon.size > 0) {
              val person =
                if (nameToID.contains(canon)) Person(pid, nameToID(canon), canon)
                else {
                  nameToID(canon) = pid
                  Person(pid, canon)
                }
              rtn = rtn + (pid -> person)
            }
          }
        }
      } catch {
        case _: SQLException => // Ignore invalid people.
      }
    }

    rtn
  }

  /**
   * Collects e-mail activity for each user over fixed intervals of time.
   * @param conn The SQL connection.
   * @param people The person directory.
   * @param range The (start, end) time stamps of the time period under consideration.
   * @param interval The number of milliseconds in each time period.
   */
  def collectMail(conn: Connection, people: TreeMap[Long, Person], range: (Long, Long), interval: Long): MailBucket = {
    val (first, last) = range
    val bucket = new MailBucket
    val cal = new GregorianCalendar

    val st = conn.createStatement
    val rs = st.executeQuery("SELECT messagedt, senderid, personid FROM recipients, messages " +
      "WHERE recipients.messageid = messages.messageid")
    while (rs.next) {
      try {
        val ts = rs.getTimestamp(1)
        val sid = rs.getInt(2)
        val rid = rs.getInt(3)

        cal.setTime(ts)
        val ms = cal.getTimeInMillis

        if ((first <= ms) && (ms <= last)) {
          (people.get(sid), people.get(rid)) match {
            case (Some(s), Some(r)) => bucket.inc(ms - ((ms - first) % interval), s.unified, r.unified)
            case _ =>
          }
        }
      } catch {
        case _: SQLException => // Ignore invalid time stamps.
      }
    }

    bucket
  }
  
  
  //-----------------------------------------------------------------------------------------------------------------------------------
  //   G E O M E T R Y
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Generate circular bar graphs in Houdini GEO format for the send/recv activity of each person. 
    * @param outdir Path to the directory where the GEO files are written.
    * @param frame The number of the frame to generate.
    * @param personal The total activity of the people to graph sorted by most to least active.
    * @param averages The average activity history (all frames) for each person. */
  def generatePersonalActivityGeo(outdir: Path, 
                                  frame: Int, 
                                  personal: TreeSet[PersonalActivity], 
                                  averages: TreeMap[Long, Array[AverageActivity]]) 
  {
    import scala.math.{ceil,log,Pi}
    val path = outdir + ("personalActivity.%04d.geo".format(frame))
    println("  Writing: " + path)
	val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val tpi = Pi * 2.0
      val tm = tpi / 180.0
      val total = personal.toList.map(_.total).reduce(_ + _).toDouble
      
      var pts = List[Pos3d]()
      var idxs = List[(Index3i,Int)]()
      
      var pc = 0      
      def arc(ta: Double, tb: Double, r0: Double, r1: Double, style: Int) {
        val c = ceil((tb - ta) / tm).toInt max 1
        for(i <- 0 to c) {
          val fr = Frame2d.rotate(Scalar.lerp(ta, tb, i.toDouble / c.toDouble)) 
          pts = (fr xform Pos2d(r0, 0.0)).toPos3d :: (fr xform Pos2d(r1, 0.0)).toPos3d :: pts
        }
        for(i <- 0 until c) {
          idxs = (Index3i(pc+1, pc+3, pc+2), style) :: (Index3i(pc, pc+1, pc+2), style) :: idxs
          pc = pc + 2
        }
        pc = pc + 2
      }
      
      val r0 = 1.0
      val r1 = 1.0075
      val r2 = 1.0125

      val ogap = tpi / 2000.0
      val igap = tpi / 2000.0
      
      val scale = 10.0
      val barlim = 0.035
      
      var off = 0L
      for(pa <- personal) {
    	val act = averages(pa.pid)(frame)
    	val (s, sclamp) = {
    	  val v = log((act.sent.toDouble / pa.sent.toDouble) + 1.0) * scale
    	  if(v > barlim) (barlim, true) else (v, false)
    	}
    	val (r, rclamp) = {
    	  val v = log((act.recv.toDouble / pa.recv.toDouble) + 1.0) * scale
    	  if(v > barlim) (barlim, true) else (v, false)
    	}
    	
        val List(ts,te) = List(off, off+pa.total).map(_.toDouble * tpi).map(_ / total)
        
        val (t0, t3) = (ts+ogap, te-ogap)
        val tr = t3 - t0 - igap
        val t1 = t0 + tr*Scalar.clamp((pa.sent.toDouble/pa.total.toDouble), 0.25, 0.7)
        val t2 = t1 + igap
        
        arc(t0, t3, r0, r1, 1)                       // Inner
        arc(t0, t1, r2, r2+s, if(sclamp) 4 else 2)   // Send
        arc(t2, t3, r2, r2+r, if(rclamp) 5 else 3)   // Recv
        
        off = off + pa.total
      }
      
      val style = "style"  // 1 = Inner, 2 = Send, 3 = Recv, 4 = Send Clamped, 5 = Recv Clamped
      val geo = GeoWriter(pts.size, idxs.size, primAttrs = List(PrimitiveIntAttr(style, 0)))
      geo.writeHeader(out)
      
      for(p <- pts.reverseIterator) geo.writePoint(out, p) 

      geo.writePrimAttrs(out)
      geo.writePolygon(out, idxs.size)
      for((i, s) <- idxs.reverseIterator) {
    	geo.setPrimAttr(style, s)
        geo.writeTriangle(out, i) 
      }
      
      geo.writeFooter(out)
    }
    finally {
      out.close
    }
  }
  
  /** Generate circular bar graphs in Houdini GEO format for the send/recv activity of each person. 
    * @param outdir Path to the directory where the GEO files are written.
    * @param sopName The name of the Geometry SOP to create to hold the labels.
    * @param personal The total activity of the people to graph sorted by most to least active.
    * @param people The names of people indexed by unique personal identifier. */
  def generatePersonalLabelHScript(outdir: Path, 
                                   sopName: String, 
                                   personal: TreeSet[PersonalActivity], 
                                   people: TreeMap[Long, Person])
  {
    import scala.math.{ceil,log,Pi}
    val path = outdir + "personalLabels.hscript"
    println("  Writing: " + path)
	val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val total = personal.toList.map(_.total).reduce(_ + _).toDouble
      val r = 1.07      
      
      out.write("opcf /obj\n" + 
                "opadd -n geo " + sopName + "\n" +
                "\n" + 
                "opcf /obj/" + sopName + "\n" + 
                "opadd -n merge AllLabels\n" + 
                "opset -d on -r on AllLabels\n") 

      var off = 0L
      var cnt = 1
      for(pa <- personal) {
        val tm = ((off.toDouble + (pa.total.toDouble * 0.5)) * 360.0) / total
        
        val font = "font" + cnt
        val xform = "xform" + cnt
        out.write("opadd -n font " + font + "\n" + 
                  "opparm " + font + " text ( '" + people(pa.pid).name + "' ) fontsize ( 0.03 ) hcenter ( off ) lod ( 1 )\n" + 
                  "opadd -n xform " + xform + "\n" +  
                  "opparm " + xform + " xOrd ( trs ) t ( %.6f 0 0 ) r ( 0 0 %.6f )\n".format(r, tm) +  
                  "opwire -n " + font + " -0 " + xform + "\n" + 
                  "opwire -n " + xform + " -" + cnt + " AllLabels\n")          
        
        off = off + pa.total
        cnt = cnt + 1
      }
    }
    finally {
      out.close
    }
  }
  
          


}