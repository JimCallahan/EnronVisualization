package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Index3i, Frame2d, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.{ PrettyPrinter, XML }
import scala.collection.immutable.{ TreeSet, TreeMap }
import scala.collection.mutable.{ HashMap, HashSet, Queue }
import scala.math.{ E, pow, log }

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader, IOException }

import scala.xml.{ Node, XML }

object GenerateRingsApp
  extends CommonIO {

  /** Top level method. */
  def main(args: Array[String]) {
    try {
      // Prefix for the family of files read and written.
      val prefix = "samples6h60fw"

      // The directory to write Houdini GEO format files.
      val geodir = Path("./artwork/houdini/geo") + prefix

      // The directory to write Houdini HScript format files.
      val hsdir = Path("./artwork/houdini/hscript")

      // The directory from which to read XML format files.
      val xmldir = Path("./data/xml")

      // The financial terms we are interested in bundling. 
      import FinancialTerm._
      val bundleTerms = Array(Litigious, Negative, Positive, Uncertainty)
      val termOffset =
        TreeMap(Litigious -> Index2i(1, 0), Uncertainty -> Index2i(1, 1),
          Negative -> Index2i(0, 0), Positive -> Index2i(0, 1))

      println("Generating Per-Person Ring Geometry...")
      val centrality = readMostCentralXML
      val people = readPeopleXML(centrality)
      generatePersonalLabelsHScript(hsdir, "personalLabels", "PeopleLabels", centrality, people)
      generateArcGeo(geodir, "personalLabelRing", centrality, 1.0, 1.0075, 0.00075)
      generateArcGeo(geodir, "personalLabelSideRing", centrality, 1.0, 1.025, 0.001)

      // The sampling window (in frames).
      val filterWidth = 30
      val window = filterWidth * 2 + 1

      // The radius of the spatial filter used to determine point density.
      val densityRadius = 0.005

      for (term <- bundleTerms) {
        val outfix = prefix + "-smooth-" + term
        val dattrName = Some(term.toString.toLowerCase + "dt")
        val aq: Queue[Array[AttrIndex]] = Queue()
        for (frame <- 60 until 3099 by 1) { 
          val (_, attrIndices) = readBundlerAttrsXML(xmldir + prefix, prefix + "-" + term, frame)
          aq.enqueue(attrIndices)
          if (aq.size > window)
            aq.dequeue
          if (aq.size == window) {
            println
            println("------ Frame " + frame + " ------")

            val battrs = aq.toArray

            // SenderID -> ReceiverID -> List(Bundler Index, Edge Index)
            val edgeIndices = HashMap[Long, HashMap[Long, List[(Int, Int)]]]()
            for ((attrIndices, bi) <- aq.zipWithIndex) {
              for ((attri, ai) <- attrIndices.zipWithIndex) {
                val rm = edgeIndices.getOrElseUpdate(attri.sendID, HashMap())
                rm += (attri.recvID -> ((bi, ai) :: rm.getOrElseUpdate(attri.recvID, List())))
              }
            }

            // Sender ID -> Total Attr Derivative
            var sattrs = TreeMap[Long, Double]()
            for ((sendID, rm) <- edgeIndices) {
              val dts =
                for ((recvID, ls) <- rm) yield {
                  def f(qi: (Int, Int)): Double = {
                    val (qbi, qei) = qi
                    val ai = battrs(qbi)(qei)
                    ai.sendAttr
                  }
                  (f(ls.last) - f(ls.head)) / ls.size.toDouble
                }
              sattrs = sattrs + (sendID -> dts.reduce(_ + _))
            }

            // Sender ID -> Total Attr Derivative
            var rattrs = TreeMap[Long, Double]()
            for ((sendID, rm) <- edgeIndices) {
              for ((recvID, ls) <- rm) yield {
                def f(qi: (Int, Int)): Double = {
                  val (qbi, qei) = qi
                  val ai = battrs(qbi)(qei)
                  ai.recvAttr
                }
                val rdt = (f(ls.last) - f(ls.head)) / ls.size.toDouble
                rattrs = rattrs + (recvID -> (rattrs.getOrElse(recvID, 0.0) + rdt))
              }
            }

            val sprefix = "sendDT-" + term + ".%04d".format(frame - filterWidth)
            generateArcGeo(geodir, sprefix, centrality, 1.035, 1.1, 0.001,
              attrName = dattrName,
              attrs = Some(centrality.keySet.toArray.map(sattrs.getOrElse(_, 0.0))))

            val rprefix = "recvDT-" + term + ".%04d".format(frame - filterWidth)
            generateArcGeo(geodir, rprefix, centrality, 1.11, 1.175, 0.001,
              attrName = dattrName,
              attrs = Some(centrality.keySet.toArray.map(rattrs.getOrElse(_, 0.0))))

            val cprefix = "totalDT-" + term + ".%04d".format(frame - filterWidth)
            generateArcGeo(geodir, cprefix, centrality, 1.01, 1.09, 0.00075, rgap = 0.0025, 
              subOffset = termOffset(term), subRange = Index2i(2), attrName = dattrName, 
              attrs = Some(centrality.keySet.toArray.map(i => sattrs.getOrElse(i, 0.0) + rattrs.getOrElse(i, 0.0))))
          }
        }
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
                     centrality: TreeMap[Long, PersonalCentrality],
                     innerRadius: Double,
                     outerRadius: Double,
                     gap: Double,
                     rgap: Double = 0.0,
                     subOffset: Index2i = Index2i(0),
                     subRange: Index2i = Index2i(1),
                     attrName: Option[String] = None,
                     attrs: Option[Array[Double]] = None) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".geo")
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val tpi = Pi * 2.0
      val tm = tpi / 180.0
      val total = centrality.values.map(_.normScore).reduce(_ + _)

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
      for ((cent, i) <- centrality.values.zipWithIndex) {
        val List(ts, te) =
          List(subOffset.x, subOffset.x + 1)
            .map(_.toDouble * (cent.normScore / subRange.x.toDouble) + off)
            .map(_ * (tpi / total))
        val List(ir, or) =
          List(subOffset.y, subOffset.y + 1)
            .map(_.toDouble / subRange.y.toDouble)
            .map(Scalar.lerp(innerRadius, outerRadius, _))
        val (gs, ge) = (if(subOffset.x == 0) 1.0 else 0.5, if(subOffset.x == subRange.x-1) 1.0 else 0.5)    
        arc(ts + tgap*gs, te - tgap*ge, ir + rgap, or - rgap, i)
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
                                    centrality: TreeMap[Long, PersonalCentrality],
                                    people: TreeMap[Long, Person]) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".hscript")
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val total = centrality.values.map(_.normScore).reduce(_ + _)
      val r = 1.07

      out.write("opcf /obj\n" +
        "opadd -n geo " + sopName + "\n" +
        "\n" +
        "opcf /obj/" + sopName + "\n" +
        "opadd -n merge AllLabels\n" +
        "opset -d on -r on AllLabels\n")

      var off = 0.0
      var cnt = 1
      for (pc <- centrality.values) {
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