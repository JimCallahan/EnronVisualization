package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Frame2d, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.{ PrettyPrinter, XML }
import scala.collection.immutable.{ TreeSet, TreeMap }
import scala.collection.mutable.{ HashMap, HashSet, Queue }
import scala.math.{ E, pow, log }

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader, IOException }

import scala.xml.{ Node, XML }

object SmoothEdgesApp
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

      // The financial terms we are interested in bundling. */
      import FinancialTerm._
      val bundleTerms = Array(Litigious, Negative, Positive, Uncertainty)

      // The sampling window (in frames).
      val filterWidth = 30
      val window = filterWidth * 2 + 1

      // The radius of the spatial filter used to determine point density.
      val densityRadius = 0.005

      for (term <- bundleTerms) {
    	val outfix = prefix + "-smooth-" + term
        val baq: Queue[(Bundler, Array[AttrIndex])] = Queue()
        for (frame <- 60 until 3099 by 1) {
          val ba @ (bundler, attrIndices) = readBundlerAttrsXML(xmldir + prefix, prefix + "-" + term, frame)
          baq.enqueue(ba)
          if (baq.size > window)
            baq.dequeue
          if (baq.size == window) {
            println
            println("------ Frame " + frame + " ------")

            val bundlers = baq.map{ case (b, _) => b}.toArray
            val battrs = baq.map{ case (_, a) => a}.toArray

            // SenderID -> ReceiverID -> List(Bundler Index, Edge Index)
            val edgeIndices = HashMap[Long, HashMap[Long, List[(Int, Int)]]]()
            for (((_, attrIndices), bi) <- baq.zipWithIndex) {
              for ((attri, ai) <- attrIndices.zipWithIndex) {
                val rm = edgeIndices.getOrElseUpdate(attri.sendID, HashMap())
                rm += (attri.recvID -> ((bi, ai) :: rm.getOrElseUpdate(attri.recvID, List())))
              }
            }

            val numEdges = edgeIndices.values.map(_.size).reduce(_ + _)
            val sbundler = Bundler(numEdges)
            val sattrs = Array.fill(numEdges)((0.0, 0.0))

            var ei = 0
            for ((sendID, rm) <- edgeIndices) {
              for ((recvID, ls) <- rm) {                
                val samples = ls.size.toDouble
                val numSegs = {
                  val (qbi, qei) = ls.head
                  bundlers(qbi).edgeSegs(qei)
                }
                
                sbundler.resizeEdge(ei, numSegs)
                for(vi <- 0 to numSegs) {
                  val sp = ls.map {
                    case (qbi, qei) => bundlers(qbi)(Index2i(qei, vi))
                  }.map(_.toVec2d).reduce(_ + _).toPos2d
                  sbundler(Index2i(ei, vi)) = sp / samples
                }
                
                val (s, r) = ls.map {
                  case (qbi, qei) => battrs(qbi)(qei)
                }.map(ai => (ai.sendAttr, ai.recvAttr))
                .reduce((a: (Double, Double), b: (Double, Double)) => (a, b) match {
                  case ((ar, as), (br, bs)) => (ar+br, as+bs)
                }) 
                sattrs(ei) = (s / samples, r / samples)
                
                ei = ei + 1
              }
            }

            generateBundleGeo(geodir, outfix, frame-filterWidth, sbundler, sattrs, densityRadius)
            println
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

  /** Construct a bundler with edges for each AverageBiSentiment.
    * @return (Bundler, (SendID, RecvID) Edge Indices)
    */
  def buildBundler(centrality: TreeMap[Long, PersonalCentrality],
                   avgBiSent: HashMap[Long, HashMap[Long, AverageBiSentiment]],
                   attrIndices: Array[AttrIndex],
                   radius: Double): Bundler = {

    import scala.math.{ ceil, log, Pi }

    val tpi = Pi * 2.0
    val tm = tpi / 180.0

    val total = centrality.values.map(_.normScore).reduce(_ + _)

    // Angles for center of each person ID.
    val theta = {
      val tm = new HashMap[Long, Double]
      var off = 0.0
      for (cent <- centrality.values) {
        tm += (cent.pid -> (((off + (cent.normScore * 0.5)) * tpi) / total))
        off = off + cent.normScore
      }
      tm
    }

    // Create single segment edges from sender to receiver IDs arranged around a circle.
    val bundler = Bundler(attrIndices.size)
    for ((ai, ei) <- attrIndices.zipWithIndex) {
      bundler.resizeEdge(ei, 1)
      for ((fr, vi) <- List(ai.sendID, ai.recvID).map(id => Frame2d.rotate(theta(id))).zipWithIndex)
        bundler(Index2i(ei, vi)) = fr xform Pos2d(1.0, 0.0)
    }

    bundler.retesselate(radius * 0.5)
  }

  /** Extract and normalize the attributes for a specific FinancialTerm with the biggest magnitude
    * for use in bundling and rendering.
    */
  def extractAttrs(maxEdges: Int,
                   term: FinancialTerm.Value,
                   bundleTerms: Array[FinancialTerm.Value],
                   avgBiSent: HashMap[Long, HashMap[Long, AverageBiSentiment]]): Array[AttrIndex] = {

    def f(snt: AverageSentiment): Double = {
      val sum = bundleTerms.map(snt.freq(_)).reduce(_ + _)
      if (sum > 0.0) snt.freq(term) / sum else 0.0
    }

    var rtn = TreeSet[AttrIndex]()
    for ((sid, rm) <- avgBiSent) {
      for ((rid, bisnt) <- rm) {
        val s = f(bisnt.send)
        val r = f(bisnt.recv)
        if ((s + r) > 0.0)
          rtn = rtn + AttrIndex(sid, rid, s, r)
      }
    }

    rtn.take(maxEdges).toArray
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

  /** Generate polygonal lines in Houdini GEO format showing the primary edge directions.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param bundler The source edges.
    */
  def generatePrimaryDirDebugGeo(outdir: Path,
                                 prefix: String,
                                 frame: Int,
                                 bundler: Bundler) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("Writing GEO File: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val numPts = bundler.numEdges * 2
      val geo = GeoWriter(numPts, bundler.numEdges)
      geo.writeHeader(out)

      for (ei <- 0 until bundler.numEdges) {
        val p = bundler(Index2i(ei, 0))
        geo.writePoint(out, p.toPos3d)
        geo.writePoint(out, (p + bundler.edgeDir(ei) * bundler.edgeLength(ei)).toPos3d)
      }

      for (ei <- 0 until bundler.numEdges)
        geo.writePolyLine(out, List(ei * 2, ei * 2 + 1))

      geo.writeFooter(out)
    }
    finally {
      out.close
    }
  }

  /** Generate polygonal lines in Houdini GEO format showing the edge midpoints.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param bundler The source edges.
    */
  def generateMidpointDebugGeo(outdir: Path,
                               prefix: String,
                               frame: Int,
                               bundler: Bundler) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("Writing GEO File: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val numPts = bundler.numEdges * 2
      val geo = GeoWriter(numPts, bundler.numEdges)
      geo.writeHeader(out)

      for (ei <- 0 until bundler.numEdges) {
        val numSegs = bundler.edgeSegs(ei)
        val a = bundler(Index2i(ei, 0))
        val b = bundler(Index2i(ei, numSegs))
        val p = Pos2d.lerp(a, b, 0.5)
        geo.writePoint(out, p.toPos3d)
        geo.writePoint(out, bundler.edgeMid(ei).toPos3d)
      }

      for (ei <- 0 until bundler.numEdges)
        geo.writePolyLine(out, List(ei * 2, ei * 2 + 1))

      geo.writeFooter(out)
    }
    finally {
      out.close
    }
  }

}