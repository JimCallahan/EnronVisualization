package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Index3i, Frame2d, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.{ PrettyPrinter, XML }
import scala.collection.immutable.{ TreeSet, TreeMap }
import scala.collection.mutable.{ HashMap, HashSet }
import scala.math.{ E, pow, log }

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader, IOException }

import scala.xml.{ Node, XML }

object GenerateRingsApp
  extends CommonIO {

  /** Top level method. */
  def main(args: Array[String]) {
    try {
      // The directory to write Houdini GEO format files.
      val geodir = Path("./artwork/houdini/geo/interpBundles")

      // The directory to write Houdini HScript format files.
      val hsdir = Path("./artwork/houdini/hscript")

      // The directory from which to read XML format files.
      val xmldir = Path("./data/xml")

      // The financial terms we are interested in bundling. 
      import FinancialTerm._
      val bundleTerms = Array(Litigious, ModalStrong, Negative, Positive, Uncertainty)

      println("Generating Per-Person Ring Geometry...")
      val centrality = readMostCentralXML
      val people = readPeopleXML(centrality)
      generatePersonalLabelsHScript(hsdir, "personalLabels", "PeopleLabels", centrality, people)
      generateArcGeo(geodir, "personalLabelRing", centrality, 1.0, 1.0075, 0.0005)
      generateArcGeo(geodir, "personalLabelSideRing", centrality, 1.0, 1.025, 0.001)

      var prevTotals = Array.fill(centrality.size)(AverageSentiment())
      for (frame <- 30 until 1545 by 1) {
        val (_, _, avgBiSent) = readBundleSentimentSamplesXML(xmldir, "averageBiSentimentSample", frame)

        val totals = {
          val rtn = Array.fill(centrality.size)(AverageSentiment())
          val bitotal = HashMap[Long, AverageSentiment]()
          for ((sid, rm) <- avgBiSent) {
            for ((rid, snt) <- rm) {
              bitotal += (sid -> (bitotal.getOrElse(sid, AverageSentiment()) + snt.send))
              bitotal += (rid -> (bitotal.getOrElse(rid, AverageSentiment()) + snt.recv))
            }
          }
          for ((person, i) <- centrality.zipWithIndex) {
            val snt = bitotal.getOrElse(person.pid, AverageSentiment())
            rtn(i) = snt.normalize(bundleTerms.map(snt.freq(_)).reduce(_ + _))
          }
          rtn
        }

        for (term <- bundleTerms) {
          val dprefix = "deriv-" + term + ".%04d".format(frame)
          val dattrName = Some(term.toString.toLowerCase + "dt")
          generateArcGeo(geodir, dprefix, centrality, 1.035, 1.1, 0.001, dattrName,
            Some((totals zip prevTotals).map { case (a, b) => a.freq(term) - b.freq(term) }))
        }

        prevTotals = totals
      }

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
  //   G E O    G E N E R A T I O N
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Generate circular ring segments for each person based on their level of centrality.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param centrality The eigenvector centrality of the people.
    * @param innerRadius The radius to the inner most edge of the generated arcs.
    * @param outerRadius The radius to the outer most edge of the generated arcs.
    * @param gap The gap between ring segments in fraction of total radius.
    * @param attrs Floating point primitive attributes to be optionally assigned to the arc for each person.
    */
  def generateArcGeo(outdir: Path,
                     prefix: String,
                     centrality: TreeSet[PersonalCentrality],
                     innerRadius: Double,
                     outerRadius: Double,
                     gap: Double,
                     attrName: Option[String] = None,
                     attrs: Option[Array[Double]] = None) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".geo")
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val tpi = Pi * 2.0
      val tm = tpi / 180.0
      val total = centrality.toList.map(_.normScore).reduce(_ + _)

      var pts: List[Pos2d] = List()
      var idxs: List[(Index3i, Int)] = List()

      var pc = 0
      def arc(ta: Double, tb: Double, r0: Double, r1: Double, attrIdx: Int) {
        val c = ceil((tb - ta) / tm).toInt max 1
        for (i <- 0 to c) {
          val fr = Frame2d.rotate(Scalar.lerp(ta, tb, i.toDouble / c.toDouble))
          pts = (fr xform Pos2d(r0, 0.0)) :: (fr xform Pos2d(r1, 0.0)) :: pts
        }
        for (i <- 0 until c) {
          idxs = (Index3i(pc + 1, pc + 3, pc + 2), attrIdx) :: (Index3i(pc, pc + 1, pc + 2), attrIdx) :: idxs
          pc = pc + 2
        }
        pc = pc + 2
      }

      val tgap = gap * tpi

      var off = 0.0
      for ((cent, i) <- centrality.zipWithIndex) {
        val List(ts, te) = List(off, off + cent.normScore).map(_ * (tpi / total))
        arc(ts + tgap, te - tgap, innerRadius, outerRadius, i)
        off = off + cent.normScore
      }

      val geo = attrName match {
        case Some(n) => GeoWriter(pts.size, idxs.size, primAttrs = List(PrimitiveFloatAttr(n, 0)))
        case _       => GeoWriter(pts.size, idxs.size)
      }
      geo.writeHeader(out)

      for (p <- pts.reverseIterator)
        geo.writePoint(out, p.toPos3d)

      if (!attrName.isEmpty) geo.writePrimAttrs(out)
      geo.writePolygon(out, idxs.size)
      for ((vis, i) <- idxs.reverseIterator) {
        (attrName, attrs) match {
          case (Some(an), Some(as)) => geo.setPrimAttr(an, as(i))
          case _                    =>
        }
        geo.writeTriangle(out, vis)
      }

      geo.writeFooter(out)
    }
    finally {
      out.close
    }
  }

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   H S C R I P T    G E N E R A T I O N
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Generate a label in Houdini HScript format for each person around the circular graph.
    * @param outdir Path to the directory where the HScript files are written.
    * @param sopName The name of the Geometry SOP to create to hold the labels.
    * @param centrality The eigenvector centrality of the people.
    * @param people The names of people indexed by unique personal identifier.
    */
  def generatePersonalLabelsHScript(outdir: Path,
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

        val label = {
          val parts = people(pc.pid).name.split("\\p{Space}")
          parts.head(0) + " " + parts.drop(1).reduce(_ + " " + _)
        }

        val font = "font" + cnt
        val xform = "xform" + cnt
        out.write("opadd -n font " + font + "\n" +
          "opparm " + font + " text ( '" + label + "' ) fontsize ( 0.03 ) hcenter ( off ) lod ( 1 )\n" +
          "opadd -n xform " + xform + "\n" +
          "opparm " + xform + " xOrd ( trs ) t ( %.6f 0 0 ) r ( 0 0 %.6f )\n".format(r, tm) +
          "opwire -n " + font + " -0 " + xform + "\n" +
          "opwire -n " + xform + " -" + cnt + " AllLabels\n")

        off = off + pc.normScore
        cnt = cnt + 1
      }
    }
    finally {
      out.close
    }
  }

}