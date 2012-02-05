package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Frame2d, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.{ PrettyPrinter, XML }
import scala.collection.immutable.{ TreeSet, TreeMap }
import scala.collection.mutable.{ HashMap, HashSet }
import scala.math.{ E, pow, log }

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader, IOException }

import scala.xml.{ Node, XML }

object InterpEdgesApp
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

      // The financial terms we are interested in bundling. */
      import FinancialTerm._
      val bundleTerms = Array(Litigious, ModalStrong, Negative, Positive, Uncertainty)

      // Whether to output per-iteration geometry.
      val debug = false

      // Radius used to compute "density" GEO attribute.
      val densityRadius = 0.01

      println("Loading Most Central People...")
      val centrality = readMostCentralXML
      val people = readPeopleXML(centrality)
      generatePersonalLabelsHScript(hsdir, "personalLabels", "PeopleLabels", centrality, people)

      // Bundle it...
      for (frame <- 30 until 1545 by 1) {
        for (term <- bundleTerms) {
          
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

  /** Generate polygonal lines in Houdini GEO format for e-mail activity and sentiment between people.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param bundler The source edges.
    * @param attrs The attributes for each end point of the edge.
    */
  def generateBundleGeo(outdir: Path,
                        prefix: String,
                        frame: Int,
                        bundler: Bundler,
                        attrs: Array[(Double, Double)],
                        densityRadius: Double) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("Writing GEO File: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val names @ List(mag, density) = List("mag", "density")
      val pointAttrs = names.map(PointFloatAttr(_, 0.0))
      val primAttrs = names.map(PrimitiveFloatAttr(_, 0.0))

      val numPts = (for (ei <- 0 until bundler.numEdges) yield bundler.edgeSegs(ei) + 3).reduce(_ + _)
      val geo = GeoWriter(numPts, bundler.numEdges, pointAttrs = pointAttrs, primAttrs = primAttrs)
      geo.writeHeader(out)

      var icnt = 0
      var idxs: List[List[Int]] = List()
      val edgeDensity = Array.fill(attrs.size)(0.0)

      val shrink = 0.925

      geo.writePointAttrs(out)
      for (ei <- 0 until attrs.size) {
        val (s, r) = attrs(ei)
        val numSegs = bundler.edgeSegs(ei)
        var eidxs: List[Int] = List()
        for (ppi <- -1 to numSegs + 1) {
          val pi = Scalar.clamp(ppi, 0, numSegs)
          val t = pi.toDouble / numSegs.toDouble
          geo.setPointAttr(mag, Scalar.lerp(s, r, t))
          val idx = Index2i(ei, pi)
          val d = bundler.vertexDensity(idx, densityRadius)
          edgeDensity(ei) = edgeDensity(ei) + d
          geo.setPointAttr(density, d)
          val pp = bundler(idx)
          val p = if ((ppi == -1) || (ppi == numSegs + 1)) pp else pp * shrink
          geo.writePoint(out, p.toPos3d)
          eidxs = icnt :: eidxs
          icnt = icnt + 1
        }
        edgeDensity(ei) = edgeDensity(ei) / numSegs.toDouble
        idxs = eidxs.reverse :: idxs
      }
      idxs = idxs.reverse

      geo.writePrimAttrs(out)
      for (ei <- 0 until attrs.size) {
        val (s, r) = attrs(ei)
        geo.setPrimAttr(mag, s + r)
        geo.setPrimAttr(density, edgeDensity(ei))
        geo.writePolyLine(out, idxs.head)
        idxs = idxs.drop(1)
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
    * @param centrality The eigenvector centrality of the people to graph sorted by score.
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