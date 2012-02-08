package org.spiffy.enron

import org.scalagfx.io.Path
import scala.xml.{ Node, PrettyPrinter, XML }

import java.util.{ Calendar, Date, GregorianCalendar }
import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader, IOException }

object GenerateTextApp
  extends CommonIO {

  /** Top level method. */
  def main(args: Array[String]) {
    try {
      // Prefix for the family of files read and written.
      val prefix = "samples6h60fw"

      // The directory to write Houdini HScript format files.
      val hsdir = Path("./artwork/houdini/hscript")

      // The directory from which to read XML format files.
      val xmldir = Path("./data/xml")

      println("Generating Date Keyframe Script...")
      val (first, last) = (90, 3068)
      val stamps = readStampsXML(xmldir, prefix, first, last)
      generateDateKeysHScript(hsdir, "date-keys", "Date", 90, stamps)

      println
      println("ALL DONE!")
    }
    catch {
      case ex =>
        println("Uncaught Exception: " + ex + "\n" +
          "Stack Trace:\n" + ex.getStackTraceString)
    }
  }

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   X M L   I N P U T 
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Write the list of validated people, returning central ones only. */
  def readStampsXML(xmldir: Path, prefix: String, first: Int, last: Int): Array[Long] = {
    val stamps =
      for (frame <- first to last) yield {
        val path = xmldir + Path(prefix) + (prefix + ".%04d.xml".format(frame))
        println("  Reading: " + path)
        val in = new BufferedReader(new FileReader(path.toFile))
        try {
          val xml = XML.load(in)
          (xml \\ "TimeStamp").head.text.toLong
        }
        finally {
          in.close
        }
      }
    stamps.toArray
  }

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   H S C R I P T    G E N E R A T I O N
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Generate an HScript with keyframes for the Day, Month and Year Switch SOPs.
    * @param outdir Path to the directory where the HScript files are written.
    * @param sopName The name of the Geometry SOP to create to hold the labels.
    * @param first The first keyframe.
    * @param stamps Time stamps for each frame.
    */
  def generateDateKeysHScript(outdir: Path,
                              prefix: String,
                              sopName: String,
                              first: Int,
                              stamps: Array[Long]) {

    val path = outdir + (prefix + ".hscript")
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      out.write("opcf /obj/" + sopName + "\n" +
        "chadd -f " + first + " " + (first + stamps.size) + " Day input\n" +
        "chadd -f " + first + " " + (first + stamps.size) + " Month input\n" +
        "chadd -f " + first + " " + (first + stamps.size) + " Year input\n")

      var pDay = -1
      var pMonth = -1
      var pYear = -1
      val cal = new GregorianCalendar
      for ((stamp, i) <- stamps.zipWithIndex) {
        cal.setTimeInMillis(stamp)
        val day = cal.get(Calendar.DAY_OF_MONTH) - 1
        val month = cal.get(Calendar.MONTH)
        val year = cal.get(Calendar.YEAR) - 2000

        if (day != pDay)
          out.write("chkey -f " + (first + i) + " -v " + day + " -F 'constant()' Day/input\n")
        pDay = day

        if (month != pMonth)
          out.write("chkey -f " + (first + i) + " -v " + month + " -F 'constant()' Month/input\n")
        pMonth = month

        if (year != pYear)
          out.write("chkey -f " + (first + i) + " -v " + year + " -F 'constant()' Year/input\n")
        pYear = year
      }
    }
    finally {
      out.close
    }
  }
}