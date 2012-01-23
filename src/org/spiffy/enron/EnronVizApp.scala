package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Pos3d, Index3i, Frame2d, Scalar }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PrimitiveIntAttr, PrimitiveFloatAttr }

import collection.mutable.HashMap
import collection.immutable.{ Queue, SortedSet, TreeMap, TreeSet }

import java.sql.{ Connection, DriverManager, ResultSet, SQLException, Timestamp }
import java.util.{ Calendar, Date, GregorianCalendar }
import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader }

object EnronVizApp {

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
            out.write("TIME STAMP,TOTAL E-MAILS\n")
            for ((stamp, cnt) <- perMonth)
              out.write(stamp + "," + cnt + "\n")
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
            out.write("PERSON ID,UNIFIED ID,NAME\n")
            for ((_, p) <- ps)
              out.write(p.pid + "," + p.unified + "," + p.name + "\n")
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
            out.write("TIME STAMP,TOTAL E-MAILS,SENT E-MAILS,RECEIVED E-MAILS\n")
            for (stamp <- samples) {
              val act = bucket.totalPeriodActivity(stamp)
              out.write(stamp + "," + act.total + "," + act.sent + "," + act.recv + "\n")
            }
          } finally {
            out.close
          }
        }

        //---------------------------------------------------------------------------------------------------

        val numPeople = 100

        println
        println("Extract Most Central (" + numPeople + ") People...")

        val mostCentral = {
          var central = new TreeSet[PersonalCentrality]

          val path = Path("./data/stats/pidsRanked.csv")
          val in = new BufferedReader(new FileReader(path.toFile))
          try {
            var done = false
            while (!done) {
              val line = in.readLine
              if (line == null) {
                done = true
              } else {
                try {
                  line.split(",") match {
                    case Array(_, p, s, _) =>
                      central = central + PersonalCentrality(p.filter(_ != '"').toLong, s.toDouble)
                    case _ =>
                  }
                } catch {
                  case _ =>
                }
              }
            }
          } finally {
            in.close
          }

          central.take(numPeople)
        }

        //---------------------------------------------------------------------------------------------------

        println
        println("Compute Personal Totals...")

        val (mostActivePersonal, mostCentralPersonal) = {
          val totalPersonal = bucket.totalPersonalActivity

          val path = Path("./data/stats/personalActivity.csv")
          println("  Writing: " + path)
          val out = new BufferedWriter(new FileWriter(path.toFile))
          try {
            out.write("PERSON ID,TOTAL E-MAILS,SENT E-MAILS,RECEIVED E-MAILS,NAME\n")
            for (pa <- totalPersonal)
              out.write(pa.pid + "," + pa.total + "," + pa.sent + "," + pa.recv + "," + people(pa.pid).name + "\n")
          } finally {
            out.close
          }

          println
          println("Extract Most Active/Central (" + numPeople + ") Personal Totals...")

          val centralIDs = mostCentral.map(_.pid)

          (totalPersonal.take(numPeople),
            totalPersonal.filter(pa => centralIDs.contains(pa.pid)))
        }

        //---------------------------------------------------------------------------------------------------

        println
        println("Collect Directional Traffic Counts...")
        val biTrafficTotals = {
          val (tt, bi) = collectTrafficTotals(conn, people, range)

          {
            val path = Path("./data/stats/trafficTotals.csv")
            println("  Writing: " + path)
            val out = new BufferedWriter(new FileWriter(path.toFile))
            try {
              out.write("SENDER ID,RECEIVER ID,TOTAL E-MAILS\n")
              for (t <- tt)
                out.write(t.sendID + "," + t.recvID + "," + t.count + "\n")
            } finally {
              out.close
            }
          }

          {
            val path = Path("./data/stats/biTrafficTotals.csv")
            println("  Writing: " + path)
            val out = new BufferedWriter(new FileWriter(path.toFile))
            try {
              out.write("PERSON-A ID,PERSON-B ID,TOTAL BIDIRECTIONAL E-MAILS\n")
              for (t <- bi)
                out.write(t.sendID + "," + t.recvID + "," + t.count + "\n")
            } finally {
              out.close
            }
          }

          def extractTrafficTotals(prefix: String, selected: Traversable[PersonalIdentified]) {
            var validID = TreeSet[Long]()
            for (pi <- selected)
              validID = validID + pi.pid

            val tt = bi.filter(t => validID.contains(t.sendID) && validID.contains(t.recvID))

            val path = Path("./data/stats/" + prefix + ".csv")
            println("  Writing: " + path)
            val out = new BufferedWriter(new FileWriter(path.toFile))
            try {
              out.write("SENDER ID,RECEIVER ID,TOTAL E-MAILS\n")
              for (t <- tt)
                out.write(t.sendID + "," + t.recvID + "," + t.count + "\n")
            } finally {
              out.close
            }
          }

          println
          println("Extract Directional Traffic of Most Active People...")

          extractTrafficTotals("mostActiveTrafficTotals", mostActivePersonal)

          println
          println("Extract Directional Traffic of Most Central People...")

          extractTrafficTotals("mostCentralTrafficTotals", mostCentralPersonal)

          bi
        }

        //---------------------------------------------------------------------------------------------------

        def extractAverageActivity(prefix: String,
                                   samples: Int,
                                   personalActivities: Traversable[PersonalIdentified]): TreeMap[Long, Array[AverageActivity]] =
          {
            var aa = new TreeMap[Long, Array[AverageActivity]]

            for (pa <- personalActivities)
              aa = aa + (pa.pid -> bucket.personalAverageActivity(pa.pid, samples))

            val path = Path("./data/stats/" + prefix + ".csv")
            println("  Writing: " + path)
            val out = new BufferedWriter(new FileWriter(path.toFile))
            try {
              val (_, first) = aa.first
              for (i <- 0 until first.size) {
                for (ary <- aa.values)
                  out.write("%.8f,".format(ary(i).total))
                out.write("\n")
              }
            } finally {
              out.close
            }

            aa
          }

        println
        println("Extract Average Activity of Most Active People...")

        val mostActiveAvgAct = extractAverageActivity("mostActiveAveragePeople", 30, mostActivePersonal)

        println
        println("Extract Average Activity of Most Central People...")

        val mostCentralAvgAct = extractAverageActivity("mostCentralAveragePeople", 30, mostCentralPersonal)

        //---------------------------------------------------------------------------------------------------

        val numFrames = 787
        
        // DUMMY FOR NOW
        val averageTraffic = {
          val rtn = new Array[TreeSet[AverageTraffic]](numFrames)

          var validID = TreeSet[Long]()
          for (pi <- mostCentralPersonal)
            validID = validID + pi.pid

          val avgtr = biTrafficTotals.filter(t => validID.contains(t.sendID) && validID.contains(t.recvID))
          for(i <- 0 until numFrames)
            rtn(i) = TreeSet[AverageTraffic]() ++ (avgtr.filter(_ => scala.math.random < 0.01).map(AverageTraffic(_, 1)))

          rtn
        }

        //---------------------------------------------------------------------------------------------------

        if (false) {
          println
          println("Generating Most Active Personal Activity Geometry...")

          for (frame <- 0 until numFrames) {
            generatePersonalActivityGeo(
              Path("./artwork/houdini/geo"), "mostActivePersonalActivity", frame,
              mostActivePersonal, mostActiveAvgAct)
          }
        }

        //---------------------------------------------------------------------------------------------------

        if (true) {
          println
          println("Generating Most Central Personal Activity Geometry...")

          for (frame <- 0 until numFrames) {
            generatePersonalActivityGeo(
              Path("./artwork/houdini/geo"), "mostCentralPersonalActivity", frame,
              mostCentral, mostCentralPersonal, mostCentralAvgAct)
          }
        }

        //---------------------------------------------------------------------------------------------------

        if (true) {
          println
          println("Generating Traffic Link Geometry...")

          for (frame <- 0 until numFrames) {
            generateTrafficGeo(
              Path("./artwork/houdini/geo"), "trafficLinks", frame,
              mostCentral, averageTraffic)
          }
        }

        //---------------------------------------------------------------------------------------------------

        println
        println("Generating Personal Label HScript...")

        generatePersonalActivityLabelHScript(
          Path("./artwork/houdini/hscript"), "mostActiveLabels", "PeopleLabels", mostActivePersonal, people)

        generatePersonalCentralityLabelHScript(
          Path("./artwork/houdini/hscript"), "mostCentralLabels", "PeopleLabels", mostCentral, people)

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

  /** Get the number of e-mails received per month (index in UTC milliseconds), ignoring those months with
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

  /** Lookup the names of all the users, discarding those without valid names.
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
              case _           => (addr, "unknown")
            }
          }

        val name =
          if (nm != null) {
            val n = nm.filter(_ != '"')
            if (n.size > 0) n else prefix
          } else prefix

        // Toss out bogus addresses.
        (name, domain) match {
          case ("e-mail", "enron.com")         =>
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

  /** Collect counts of directional e-mail traffic between individual pairs of people.
    * E-Mails from or to people not included in the directory will be silently ignored.
    * Bi-directional exchanges (returned as the second list) is the minimum of the Traffic.count for
    * e-mails exchanged from person A to B and person B to A.  The results list will contain pairs
    * of symmetric Traffic entries with identical counts between any two people included.
    * @param conn The SQL connection.
    * @param people The person directory.
    * @param range The (start, end) time stamps of the time period under consideration.
    * @return The one-way traffic totals between all people and the bi-directional exchange traffic totals.
    */
  def collectTrafficTotals(conn: Connection,
                           people: TreeMap[Long, Person],
                           range: (Long, Long)): (List[Traffic], List[Traffic]) = {
    val (first, last) = range
    val table = new HashMap[Long, HashMap[Long, Long]]
    val cal = new GregorianCalendar

    val st = conn.createStatement
    val rs = st.executeQuery("SELECT messagedt, senderid, personid FROM recipients, messages " +
      "WHERE recipients.messageid = messages.messageid")
    while (rs.next) {
      try {
        val ts = rs.getTimestamp(1)
        val sid = rs.getInt(2).toLong
        val rid = rs.getInt(3).toLong

        cal.setTime(ts)
        val ms = cal.getTimeInMillis

        if ((first <= ms) && (ms <= last) && people.contains(sid)) {
          val sendID = people(sid).unified
          val receivers = table.getOrElseUpdate(sendID, new HashMap[Long, Long])
          receivers += (rid -> (receivers.getOrElse(rid, 0L) + 1L))
        }
      } catch {
        case _: SQLException => // Ignore invalid people.
      }
    }

    val biTable = {
      val bidir = new HashMap[Long, HashMap[Long, Long]]
      for ((sid, rm) <- table; (rid, srCount) <- rm) {
        if (table.contains(rid)) {
          val sm = table(rid)
          if (sm.contains(sid)) {
            val rsCount = sm(sid)
            bidir.getOrElseUpdate(sid, new HashMap[Long, Long]) += (rid -> (srCount min rsCount))
          }
        }
      }
      bidir
    }

    def results(hm: HashMap[Long, HashMap[Long, Long]]) = {
      var rtn: List[Traffic] = List()
      for ((sid, rm) <- hm; (rid, cnt) <- rm)
        rtn = Traffic(sid, rid, cnt) :: rtn
      rtn
    }

    (results(table), results(biTable))
  }

  /** Collects e-mail activity for each user over fixed intervals of time.
    * @param conn The SQL connection.
    * @param people The person directory.
    * @param range The (start, end) time stamps of the time period under consideration.
    * @param interval The number of milliseconds in each time period.
    */
  def collectMail(conn: Connection,
                  people: TreeMap[Long, Person],
                  range: (Long, Long),
                  interval: Long): MailBucket = {
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
            case _                  =>
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
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param personal The total activity of the people to graph sorted by most to least active.
    * @param averages The average activity history (all frames) for each person.
    */
  def generatePersonalActivityGeo(outdir: Path,
                                  prefix: String,
                                  frame: Int,
                                  personal: TreeSet[PersonalActivity],
                                  averages: TreeMap[Long, Array[AverageActivity]]) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val tpi = Pi * 2.0
      val tm = tpi / 180.0
      val total = personal.toList.map(_.total).reduce(_ + _).toDouble

      var pts = List[Pos3d]()
      var idxs = List[(Index3i, Int)]()

      var pc = 0
      def arc(ta: Double, tb: Double, r0: Double, r1: Double, style: Int) {
        val c = ceil((tb - ta) / tm).toInt max 1
        for (i <- 0 to c) {
          val fr = Frame2d.rotate(Scalar.lerp(ta, tb, i.toDouble / c.toDouble))
          pts = (fr xform Pos2d(r0, 0.0)).toPos3d :: (fr xform Pos2d(r1, 0.0)).toPos3d :: pts
        }
        for (i <- 0 until c) {
          idxs = (Index3i(pc + 1, pc + 3, pc + 2), style) :: (Index3i(pc, pc + 1, pc + 2), style) :: idxs
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
      for (pa <- personal) {
        val act = averages(pa.pid)(frame)
        val (s, sclamp) = {
          val v = log((act.sent.toDouble / pa.sent.toDouble) + 1.0) * scale
          if (v > barlim) (barlim, true) else (v, false)
        }
        val (r, rclamp) = {
          val v = log((act.recv.toDouble / pa.recv.toDouble) + 1.0) * scale
          if (v > barlim) (barlim, true) else (v, false)
        }

        val List(ts, te) = List(off, off + pa.total).map(_.toDouble * tpi).map(_ / total)

        val (t0, t3) = (ts + ogap, te - ogap)
        val tr = t3 - t0 - igap
        val t1 = t0 + tr * Scalar.clamp((pa.sent.toDouble / pa.total.toDouble), 0.25, 0.7)
        val t2 = t1 + igap

        arc(t0, t3, r0, r1, 1) // Inner
        arc(t0, t1, r2, r2 + s, if (sclamp) 4 else 2) // Send
        arc(t2, t3, r2, r2 + r, if (rclamp) 5 else 3) // Recv

        off = off + pa.total
      }

      val style = "style" // 1 = Inner, 2 = Send, 3 = Recv, 4 = Send Clamped, 5 = Recv Clamped
      val geo = GeoWriter(pts.size, idxs.size, primAttrs = List(PrimitiveIntAttr(style, 0)))
      geo.writeHeader(out)

      for (p <- pts.reverseIterator) geo.writePoint(out, p)

      geo.writePrimAttrs(out)
      geo.writePolygon(out, idxs.size)
      for ((i, s) <- idxs.reverseIterator) {
        geo.setPrimAttr(style, s)
        geo.writeTriangle(out, i)
      }

      geo.writeFooter(out)
    } finally {
      out.close
    }
  }

  /** Generate circular bar graphs in Houdini GEO format for the send/recv activity of each person.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param centrality The eigenvector centrality of the people to graph sorted by score.
    * @param personal The total activity of the most central people to graph sorted by most to least active.
    * @param averages The average activity history (all frames) for each person.
    */
  def generatePersonalActivityGeo(outdir: Path,
                                  prefix: String,
                                  frame: Int,
                                  centrality: TreeSet[PersonalCentrality],
                                  personal: TreeSet[PersonalActivity],
                                  averages: TreeMap[Long, Array[AverageActivity]]) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val tpi = Pi * 2.0
      val tm = tpi / 180.0
      val total = centrality.toList.map(_.normScore).reduce(_ + _)

      var pts = List[Pos3d]()
      var idxs = List[(Index3i, Int)]()

      var pc = 0
      def arc(ta: Double, tb: Double, r0: Double, r1: Double, style: Int) {
        val c = ceil((tb - ta) / tm).toInt max 1
        for (i <- 0 to c) {
          val fr = Frame2d.rotate(Scalar.lerp(ta, tb, i.toDouble / c.toDouble))
          pts = (fr xform Pos2d(r0, 0.0)).toPos3d :: (fr xform Pos2d(r1, 0.0)).toPos3d :: pts
        }
        for (i <- 0 until c) {
          idxs = (Index3i(pc + 1, pc + 3, pc + 2), style) :: (Index3i(pc, pc + 1, pc + 2), style) :: idxs
          pc = pc + 2
        }
        pc = pc + 2
      }

      val r0 = 1.0
      val r1 = 1.0075
      val r2 = 1.0125

      val ogap = tpi / 2000.0
      val igap = ogap * 0.5

      val scale = 10.0
      val barlim = 0.035

      val paTable =
        ((new TreeMap[Long, PersonalActivity]) /: personal) {
          case (rtn, pa) => rtn + (pa.pid -> pa)
        }

      var off = 0.0
      for (cent <- centrality) {
        val xpa = paTable(cent.pid)

        val act = averages(cent.pid)(frame)
        val (s, sclamp) = {
          val v = log((act.sent.toDouble / xpa.sent.toDouble) + 1.0) * scale
          if (v > barlim) (barlim, true) else (v, false)
        }
        val (r, rclamp) = {
          val v = log((act.recv.toDouble / xpa.recv.toDouble) + 1.0) * scale
          if (v > barlim) (barlim, true) else (v, false)
        }

        val List(ts, te) = List(off, off + cent.normScore).map(_ * (tpi / total))

        val (t0, t3) = (ts + ogap, te - ogap)
        val tr = t3 - t0 - igap
        val tm = Scalar.lerp(t0, t3, 0.5)
        val t1 = tm - igap
        val t2 = tm + igap

        arc(t0, t3, r0, r1, 1) // Inner
        arc(t0, t1, r2, r2 + s, if (sclamp) 4 else 2) // Send
        arc(t2, t3, r2, r2 + r, if (rclamp) 5 else 3) // Recv

        off = off + cent.normScore
      }

      val style = "style" // 1 = Inner, 2 = Send, 3 = Recv, 4 = Send Clamped, 5 = Recv Clamped
      val geo = GeoWriter(pts.size, idxs.size, primAttrs = List(PrimitiveIntAttr(style, 0)))
      geo.writeHeader(out)

      for (p <- pts.reverseIterator) geo.writePoint(out, p)

      geo.writePrimAttrs(out)
      geo.writePolygon(out, idxs.size)
      for ((i, s) <- idxs.reverseIterator) {
        geo.setPrimAttr(style, s)
        geo.writeTriangle(out, i)
      }

      geo.writeFooter(out)
    } finally {
      out.close
    }
  }

  /** Generate polygonal lines in Houdini GEO format for traffic between people.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param centrality The eigenvector centrality of the people to graph sorted by score.
    * @param averages The average traffic on each frame of the history sorted by traffic count
    * (zero count entries are ommitted).
    */
  def generateTrafficGeo(outdir: Path,
                         prefix: String,
                         frame: Int,
                         centrality: TreeSet[PersonalCentrality],
                         averages: Array[TreeSet[AverageTraffic]]) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val tpi = Pi * 2.0
      val tm = tpi / 180.0
      val total = centrality.toList.map(_.normScore).reduce(_ + _)

      // angles for center of each pid
      val theta = {
        val tm = new HashMap[Long, Double]
        var off = 0.0
        for (cent <- centrality) {
          tm += (cent.pid -> (((off + (cent.normScore * 0.5)) * tpi) / total))
          off = off + cent.normScore
        }
        tm
      }

      var pts = List[Pos3d]()
      var idxs = List[(List[Int], Double)]()

      val half = List(Pos2d(1.0, 0.0), Pos2d(0.5, 0.0))
      val rhalf = half.reverse

      var pc = 0
      for (tr <- averages(frame)) {
        val List(fr, rfr) = List(tr.sendID, tr.recvID).map(id => Frame2d.rotate(theta(id)))
        for (p <- half)
          pts = (fr xform p).toPos3d :: pts
        for (p <- rhalf)
          pts = (rfr xform p).toPos3d :: pts

        idxs = (List(pc, pc + 1, pc + 2, pc + 3), tr.count) :: idxs
        pc = pc + 4
      }

      val traffic = "traffic" 
      val geo = GeoWriter(pts.size, idxs.size, primAttrs = List(PrimitiveFloatAttr(traffic, 0)))
      geo.writeHeader(out)
      
      for (p <- pts.reverseIterator) geo.writePoint(out, p)
      
      geo.writePrimAttrs(out)
      for ((idx, c) <- idxs.reverseIterator) {
        geo.setPrimAttr(traffic, c)
        geo.writePolyLine(out, idx)
      }

      geo.writeFooter(out)

    } finally {
      out.close
    }
  }

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   H S C R I P T
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Generate a label in Houdini HScript format for each person around the circular graph.
    * @param outdir Path to the directory where the HScript files are written.
    * @param sopName The name of the Geometry SOP to create to hold the labels.
    * @param personal The total activity of the people to graph sorted by most to least active.
    * @param people The names of people indexed by unique personal identifier.
    */
  def generatePersonalActivityLabelHScript(outdir: Path,
                                           prefix: String,
                                           sopName: String,
                                           personal: TreeSet[PersonalActivity],
                                           people: TreeMap[Long, Person]) {
    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".hscript")
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
      for (pa <- personal) {
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
    } finally {
      out.close
    }
  }

  /** Generate a label in Houdini HScript format for each person around the circular graph.
    * @param outdir Path to the directory where the HScript files are written.
    * @param sopName The name of the Geometry SOP to create to hold the labels.
    * @param centrality The eigenvector centrality of the people to graph sorted by score.
    * @param people The names of people indexed by unique personal identifier.
    */
  def generatePersonalCentralityLabelHScript(outdir: Path,
                                             prefix: String,
                                             sopName: String,
                                             centrality: TreeSet[PersonalCentrality],
                                             people: TreeMap[Long, Person]) {
    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".hscript")
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val total = centrality.toList.map(_.normScore).reduce(_ + _)
      val r = 1.07

      out.write("opcf /obj\n" +
        "opadd -n geo " + sopName + "\n" +
        "\n" +
        "opcf /obj/" + sopName + "\n" +
        "opadd -n merge AllLabels\n" +
        "opset -d on -r on AllLabels\n")

      var off = 0.0
      var cnt = 1
      for (pc <- centrality) {
        val tm = ((off.toDouble + pc.normScore * 0.5) * 360.0) / total

        val font = "font" + cnt
        val xform = "xform" + cnt
        out.write("opadd -n font " + font + "\n" +
          "opparm " + font + " text ( '" + people(pc.pid).name + "' ) fontsize ( 0.03 ) hcenter ( off ) lod ( 1 )\n" +
          "opadd -n xform " + xform + "\n" +
          "opparm " + xform + " xOrd ( trs ) t ( %.6f 0 0 ) r ( 0 0 %.6f )\n".format(r, tm) +
          "opwire -n " + font + " -0 " + xform + "\n" +
          "opwire -n " + xform + " -" + cnt + " AllLabels\n")

        off = off + pc.normScore
        cnt = cnt + 1
      }
    } finally {
      out.close
    }
  }

  /** Generate a label in Houdini HScript format
    * @param outdir Path to the directory where the HScript files are written.
    * @param sopName The name of the Geometry SOP to create to hold the labels.
    * @param range The (start, end) time stamps of the time period under consideration.
    * @param interval The number of milliseconds in each time period.
    */
  def generateDateLabelHScript(outdir: Path,
                               prefix: String,
                               sopName: String,
                               range: (Long, Long),
                               interval: Long) {
    val path = outdir + (prefix + ".hscript")
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {

    } finally {
      out.close
    }
  }

}