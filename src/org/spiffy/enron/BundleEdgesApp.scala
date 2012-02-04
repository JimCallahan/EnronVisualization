package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Frame2d, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.{ PrettyPrinter, XML }
import scala.collection.immutable.{ TreeSet }
import scala.collection.mutable.{ HashMap, HashSet }
import scala.math.{ E, pow, log }

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader, IOException }

object BundleEdgesApp {

  /** An sortable attribute value and associated sender/receiver IDs. */
  class AttrIndex private (val sendID: Long, val recvID: Long, val sendAttr: Double, val recvAttr: Double)
    extends Ordered[AttrIndex] {
    /** Ordered in decreasing attribute value and increasing send/recv IDs. */
    def compare(that: AttrIndex): Int =
      (that.total compare total) match {
        case 0 => (sendID compare that.sendID) match {
          case 0  => recvID compare that.recvID
          case c2 => c2
        }
        case c => c
      }

    def total = sendAttr + recvAttr
  }

  object AttrIndex {
    def apply(sendID: Long, recvID: Long, sendAttr: Double, recvAttr: Double) =
      new AttrIndex(sendID, recvID, sendAttr, recvAttr)
  }

  /** Top level method. */
  def main(args: Array[String]) {
    try {
      // The directory to write Houdini GEO format files.
      val geodir = Path("./artwork/houdini/geo/interpBundles")

      // The directory from which to read XML format files.
      val xmldir = Path("./data/xml")

      // The financial terms we are interested in bundling. */
      import FinancialTerm._
      val bundleTerms = Array(Litigious, ModalStrong, Negative, Positive, Uncertainty)

      // Whether to output per-iteration geometry.
      val debug = false

      // Iteration controls.
      val iterations = 300
      val converge = 0.05
      val springConst = 7.0
      val electroConst = 0.35
      val radius = 0.2
      val minCompat = 0.1
      val step = Interval(1E-6, 0.01)
      val densityRadius = 0.01

      println("Loading Most Central People...")
      val centrality = readMostCentralXML

      // Bundle it...
      for (term <- bundleTerms) {
        println
        println("Processing " + term + " Bundles...")
        for (frame <- 30 until 1545 by 15) {

          val (stamp, interval, avgBiSent) = readBundleSentimentSamplesXML(xmldir, frame)
          val attrIndices = extractAttrs(term, bundleTerms, avgBiSent)
          if (!attrIndices.isEmpty) {
            val bundler = buildBundler(centrality, avgBiSent, attrIndices, radius)

            val attrs = attrIndices.map(ai => (ai.sendAttr, ai.recvAttr))

            bundler.prepare
            for (i <- 0 until iterations) {
              if (debug) generateBundleGeo(geodir, "iter." + i + "." + term, frame, bundler, attrs, densityRadius)
              bundler.iterate(springConst, electroConst, radius, minCompat, Some(attrs), converge, step)
            }
            if (!debug) println

            // Write out the GEO results.
            generateBundleGeo(geodir, "bundled-" + term, frame, bundler, attrs, densityRadius)
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
  def buildBundler(centrality: TreeSet[PersonalCentrality],
                   avgBiSent: HashMap[Long, HashMap[Long, AverageBiSentiment]],
                   attrIndices: Array[AttrIndex],
                   radius: Double): Bundler = {

    import scala.math.{ ceil, log, Pi }

    val tpi = Pi * 2.0
    val tm = tpi / 180.0
    val total = centrality.toList.map(_.normScore).reduce(_ + _)

    // Angles for center of each person ID.
    val theta = {
      val tm = new HashMap[Long, Double]
      var off = 0.0
      for (cent <- centrality) {
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
  def extractAttrs(term: FinancialTerm.Value,
                   bundleTerms: Array[FinancialTerm.Value],
                   avgBiSent: HashMap[Long, HashMap[Long, AverageBiSentiment]]): Array[AttrIndex] = {

    def f(snt: AverageSentiment): Double =
      snt.freq(term) / bundleTerms.map(snt.freq(_)).reduce(_ + _)

    val numEdges = avgBiSent.map { case (_, rm) => rm.size }.reduce(_ + _)
    var rtn = TreeSet[AttrIndex]()
    for ((sid, rm) <- avgBiSent) {
      for ((rid, bisnt) <- rm) {
        val s = f(bisnt.send)
        val r = f(bisnt.recv)
        if ((s + r) > 0.0)
          rtn = rtn + AttrIndex(sid, rid, s, r)
      }
    }

    rtn.take(rtn.size / 5).toArray
  }

  //-----------------------------------------------------------------------------------------------------------------------------------
  //   X M L   I N P U T 
  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Read the most central (eigenvector centrality) people. */
  def readMostCentralXML: TreeSet[PersonalCentrality] = {
    val path = Path("./data/xml/mostCentral.csv")
    println("  Reading: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val xml = XML.load(in)
      var central = TreeSet[PersonalCentrality]()
      for (pc <- xml \\ "PersonalCentrality")
        central = central + PersonalCentrality.fromXML(pc)
      central
    }
    finally {
      in.close
    }
  }

  /** Read in edges and associated attributes.
    * @return (Time Stamp, Interval, Samples)
    */
  def readBundleSentimentSamplesXML(xmldir: Path, frame: Int): (Long, Long, HashMap[Long, HashMap[Long, AverageBiSentiment]]) = {
    //val prefix = "bundleSentimentSample"
    val prefix = "averageBiSentimentSample"
    val path = xmldir + Path(prefix) + (prefix + ".%04d.xml".format(frame))
    println("Reading XML File: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val xml = XML.load(in)

      val xframe = (xml \\ "Frame").text.toInt
      if (frame != xframe) throw new IOException("Wrong input frame!")
      else {
        val stamp = (xml \\ "TimeStamp").text.toLong
        val interval = (xml \\ "Interval").text.toLong
        val rtn = HashMap[Long, HashMap[Long, AverageBiSentiment]]()
        for (s <- xml \\ "AvgBiSent") {
          val snt = AverageBiSentiment.fromXML(s)
          val sm = rtn.getOrElseUpdate(snt.sendID, HashMap[Long, AverageBiSentiment]())
          sm += (snt.recvID -> snt)
        }

        (stamp, interval, rtn)
      }
    }
    finally {
      in.close
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