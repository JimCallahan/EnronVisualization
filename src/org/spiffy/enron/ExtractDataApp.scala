package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Pos3d, Index2i, Index3i, Frame2d, Scalar }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveIntAttr }

import collection.mutable.{ HashMap, HashSet }
import collection.immutable.{ SortedSet, TreeMap, TreeSet }
import scala.xml.{ PrettyPrinter, XML }

import java.sql.{ Connection, DriverManager, ResultSet, SQLException, Timestamp }
import java.util.{ Calendar, Date, GregorianCalendar }
import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader }

object ExtractDataApp
  extends CommonIO {

  // Loads the JDBC driver. 
  classOf[com.mysql.jdbc.Driver]

  /** Top level method. */
  def main(args: Array[String]) {
    try {
      val cal = new GregorianCalendar
      val connStr = "jdbc:mysql://localhost:3306/enron?user=enron&password=slimyfucks"
      val conn = DriverManager.getConnection(connStr)
      try {
        // Number of people to display in the graph.
        val numPeople = 100

        // The directory to write XML format files.
        val xmldir = Path("./data/xml")

        println("Determining the Active Interval...")
        val samples = {
          val interval = 6 * 60 * 60 * 1000 // 6-hours
          val threshold = 10000
          val mt = collectActiveMonths(conn, threshold)
          writeMonthlyTotalsXML(mt)
          Samples(mt.firstKey, mt.lastKey, interval)
        }

        // The sampling window (in frames).
        val filterWidth = 60
        val window = filterWidth * 2 + 1

        println
        println("Collecting People...")
        val people = collectPeople(conn)
        writePeopleXML(people)

        println
        println("Extract Most Central (" + numPeople + ") People...")
        val mostCentral = readMostCentral(numPeople)
        writeMostCentralXML(mostCentral)

        println
        println("Collecting Sentiment...")
        val bucket = collectSentiment(conn, people, mostCentral.map(_.pid), samples)

        println
        print("Averaging Sentiment: ")
        val avgBiSentSamples = bucket.averageBiSentiment(window)
        
        println
        val prefix = "samples6h60fw" 
        writeAvergeBiSentimentSamplesXML(xmldir, prefix, samples, filterWidth, avgBiSentSamples)

        println
        println("ALL DONE!")
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

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   D A T A B A S E 
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Get the number of e-mails received per month (index in UTC milliseconds), ignoring those months with
    * less activity than the given threshold number of emails.
    * @param conn The SQL connection.
    * @param threshold The minimum amount of e-mail activity.
    */
  def collectActiveMonths(conn: Connection, threshold: Long): TreeMap[Long, Long] = {
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
      }
      catch {
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
        val pid = rs.getLong(1)
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
          }
          else prefix

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
      }
      catch {
        case _: SQLException => // Ignore invalid people.
      }
    }

    rtn
  }

  /** Collects e-mail activity for each user over fixed intervals of time.
    * @param conn The SQL connection.
    * @param people The person directory.
    * @param samples The sampling range and interval.
    */
  def collectSentiment(conn: Connection,
                       people: TreeMap[Long, Person],
                       mostCentral: TreeSet[Long],
                       samples: Samples): SentimentBucket = {

    val bucket = SentimentBucket(samples)
    val cal = new GregorianCalendar

    val messageIDs = new HashSet[Long]()

    {
      println("  Collecting Message Counts...")
      val st = conn.createStatement
      val rs = st.executeQuery(
        "SELECT messagedt, senderid, personid, recipients.messageid FROM recipients, messages " +
          "WHERE recipients.messageid = messages.messageid")
      while (rs.next) {
        try {
          val ts = rs.getTimestamp(1)
          val sid = rs.getLong(2)
          val rid = rs.getLong(3)
          val mid = rs.getLong(4)

          cal.setTime(ts)
          val ms = cal.getTimeInMillis

          if (samples.inRange(ms)) {
            (people.get(sid), people.get(rid)) match {
              case (Some(s), Some(r)) =>
                if (mostCentral.contains(s.unified) && mostCentral.contains(r.unified)) {
                  bucket.inc(samples.intervalStart(ms), s.unified, r.unified, mid.toLong)
                  messageIDs += mid
                }
              case _ =>
            }
          }
        }
        catch {
          case _: SQLException => // Ignore invalid time stamps.
        }
      }
    }

    val path = Path("./data/xml/financialDictionary.xml")
    val dict = FinancialDictionary.loadXML(path)
    import FinancialTerm._

    println("  Classifying E-Mail Text...")
    var cnt = 0L
    for (mid <- messageIDs) {
      val st = conn.prepareStatement("SELECT body FROM bodies WHERE messageid = ?")
      st.setLong(1, mid)
      val rs = st.executeQuery
      while (rs.next) {
        try {
          val msg = rs.getString(1)
          bucket.addTerms(mid, msg.split("\\p{Space}").map(dict.classify _))
        }
        catch {
          case _: SQLException => // Ignore invalid time stamps.
        }
      }
    }

    println("  Collating Results...")
    bucket.collate

    bucket
  }

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   X M L   O U T P U T 
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Write the total e-mails sent each month. */
  def writeMonthlyTotalsXML(perMonth: TreeMap[Long, Long]) {
    val path = Path("./data/xml/monthlyTotals.xml")
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val xml =
        <MonthlyTotals>{
          perMonth.map {
            case (stamp, count) => <Month><TimeStamp>{ stamp }</TimeStamp><Count>{ count }</Count></Month>
          }
        }</MonthlyTotals>

      val pp = new PrettyPrinter(100, 2)
      out.write(pp.format(xml))
    }
    finally {
      out.close
    }
  }

  /** Generate the curves for the average traffic between the most central people.
    * @param xmldir The XML output file directory.
    * @param samples The sampling range and interval.
    * @param avgSents:
    */
  def writeAvergeBiSentimentSamplesXML(xmldir: Path,
                                       prefix: String,
                                       samples: Samples,
                                       filterWidth: Int,
                                       avgSents: Array[TreeSet[AverageBiSentiment]]) {
    val numFrames = avgSents.size
    def toFrame(i: Int) = i + filterWidth

    val dir = xmldir + prefix

    println
    println("Generating AverageBiSentiment Sample XML...")
    println("  XML Files: " + (dir + prefix) + ".%04d-%04d.xml".format(toFrame(0), toFrame(numFrames) - 1))
    print("  Writing: [" + toFrame(0) + "] ")
    for (i <- 0 until numFrames) {
      val frame = toFrame(i)
      val path = dir + (prefix + ".%04d.xml".format(frame))
      val out = new BufferedWriter(new FileWriter(path.toFile))
      try {
        val xml =
          <AverageBiSentimentSample>
            <Frame>{ frame }</Frame>
            <TimeStamp>{ samples(frame) }</TimeStamp>
            <Interval>{ samples.interval }</Interval>
            <AvgBiSents>{ avgSents(i).toList.map(_.toXML) }</AvgBiSents>
          </AverageBiSentimentSample>

        val pp = new PrettyPrinter(100, 2)
        out.write(pp.format(xml))
      }
      finally {
        out.close
      }
      if (frame % 100 == 99) print(" [" + (frame + 1) + "]\n           ") else print(".")
    }
    println(" [" + (toFrame(numFrames) - 1) + "] -- DONE.")
  }

  /** Generate the curves for the average traffic between the most central people.
    * @param xmldir The XML output file directory.
    * @param samples The sampling range and interval.
    * @param avgSents:
    */
  def writeBundleSentimentSamplesXML(xmldir: Path,
                                     samples: Samples,
                                     filterWidth: Int,
                                     avgSents: Array[TreeSet[AverageBiSentiment]]) {
    val numFrames = avgSents.size
    def toFrame(i: Int) = i + filterWidth
    val startIdx = filterWidth
    val lastIdx = startIdx + ((numFrames / filterWidth) - 2) * filterWidth

    val prefix = "bundleSentimentSample"
    val dir = xmldir + prefix

    println
    println("Generating Bundle AverageBiSentiment Sample XML...")
    println("  XML Files: " + (dir + prefix) +
      ".%04d-%04dx%04d.xml".format(toFrame(startIdx), toFrame(lastIdx), filterWidth))
    print("  Writing: [" + toFrame(startIdx) + "] ")
    for (i <- startIdx to lastIdx by filterWidth) {
      val frame = toFrame(i)
      val path = dir + (prefix + ".%04d.xml".format(frame))
      val out = new BufferedWriter(new FileWriter(path.toFile))
      try {
        val merged = new HashMap[Long, HashMap[Long, AverageBiSentiment]]()
        for (as <- List(avgSents(i - filterWidth), avgSents(i), avgSents(i + filterWidth))) {
          for (a <- as) {
            val rm = merged.getOrElseUpdate(a.sendID, new HashMap[Long, AverageBiSentiment])
            val x =
              rm.get(a.recvID) match {
                case Some(p) => p + a
                case _       => a
              }
            rm += (a.recvID -> x)
          }
        }

        val xml =
          <AverageBiSentimentSample>
            <Frame>{ frame }</Frame>
            <TimeStamp>{ samples(frame) }</TimeStamp>
            <Interval>{ samples.interval }</Interval>
            <AvgBiSents>{
              (merged.map { case (_, rm) => rm.map { case (_, s) => s.toXML } }).flatten
            }</AvgBiSents>
          </AverageBiSentimentSample>

        val pp = new PrettyPrinter(100, 2)
        out.write(pp.format(xml))
      }
      finally {
        out.close
      }
      if (frame % (100 * filterWidth) == (100 * filterWidth - 1))
        print(" [" + (frame + 1) + "]\n           ")
      else print(".")
    }
    println(" [" + toFrame(lastIdx) + "] -- DONE.")
  }

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   C S V   
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Read the most central (eigenvector centrality) people computed externally with R. */
  def readMostCentral(numPeople: Int): TreeSet[PersonalCentrality] = {
    var central = new TreeSet[PersonalCentrality]

    val path = Path("./data/source/pidsRanked.csv")
    println("  Reading: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      var done = false
      while (!done) {
        val line = in.readLine
        if (line == null) {
          done = true
        }
        else {
          try {
            line.split(",") match {
              case Array(_, p, s, _) =>
                central = central + PersonalCentrality(p.filter(_ != '"').toLong, s.toDouble)
              case _ =>
            }
          }
          catch {
            case _ =>
          }
        }
      }
    }
    finally {
      in.close
    }

    central.take(numPeople)
  }

}