package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.{ PrettyPrinter, XML }

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader }

object EdgeBundlerApp {
  /** Top level method. */
  def main(args: Array[String]) {
    try {
      // The directory to write Houdini GEO format files.
      val geodir = Path("./artwork/houdini/geo")

      // The directory to write XML format files.
      val xmldir = Path("./data/xml/sentiment")

      // Prefix of input XML files
      val prefix = "sentimentLinks"

      //Attribute names. */
      val attrNames = Array("litigious", "modalstrong", "negative", "positive", "uncertainty")

      val passes = 1
      val iterations = 60
      val converge = 0.0025
      val springConst = 6.0
      val electroConst = 0.15
      val radius = 0.2
      val minCompat = 0.0
      val step = Interval(1E-8, 0.01)

      val densityRadius = 0.01

      println
      var icnt = 0
      for (attrName <- attrNames) {
        for (frame <- 1200 until 4200 by 100) {
          val (bundler, attrs) = readSentimentXML(xmldir, attrName, frame)

          /*
        val (bundler, attrs) = {
          val b = ForceDirectedEdgeBundler(9)

          b.resizeEdge(0, 1)
          b(Index2i(0, 0)) = Vec2d(-0.9, -0.05).normalized.toPos2d
          b(Index2i(0, 1)) = Vec2d(0.9, -0.05).normalized.toPos2d

          b.resizeEdge(1, 1)
          b(Index2i(1, 0)) = Vec2d(-0.9, 0.05).normalized.toPos2d
          b(Index2i(1, 1)) = Vec2d(0.9, 0.05).normalized.toPos2d

          b.resizeEdge(2, 1)
          b(Index2i(2, 0)) = Vec2d(-1.0, 1.0).normalized.toPos2d
          b(Index2i(2, 1)) = Vec2d(-1.0, -1.0).normalized.toPos2d

          b.resizeEdge(3, 1)
          b(Index2i(3, 0)) = Vec2d(-1.0, 1.0).normalized.toPos2d
          b(Index2i(3, 1)) = Vec2d(1.0, -1.0).normalized.toPos2d

          b.resizeEdge(4, 1)
          b(Index2i(4, 0)) = Vec2d(-0.8, 1.0).normalized.toPos2d
          b(Index2i(4, 1)) = Vec2d(1.0, -1.0).normalized.toPos2d

          b.resizeEdge(5, 1)
          b(Index2i(5, 0)) = Vec2d(0.8, 0.2).normalized.toPos2d
          b(Index2i(5, 1)) = Vec2d(0.4, 0.5).normalized.toPos2d

          b.resizeEdge(6, 1)
          b(Index2i(6, 0)) = Vec2d(0.2, 0.8).normalized.toPos2d
          b(Index2i(6, 1)) = Vec2d(0.5, 0.4).normalized.toPos2d

          b.resizeEdge(7, 1)
          b(Index2i(7, 0)) = Vec2d(0.1, 0.7).normalized.toPos2d
          b(Index2i(7, 1)) = Vec2d(0.4, 0.3).normalized.toPos2d

          b.resizeEdge(8, 1)
          b(Index2i(8, 0)) = Vec2d(-0.9, 0.15).normalized.toPos2d
          b(Index2i(8, 1)) = Vec2d(0.9, 0.15).normalized.toPos2d

          (b, Array.fill(b.numEdges)((0.0, 0.0)))
        }
        */

          var result = bundler.retesselate(radius * 0.5)
          result.prepare
          //generateSentimentGeo(geodir, "bundler-prepass", frame, result, attrs, densityRadius)
          //generatePrimaryDirDebugGeo(geodir, "bundler-primdirs", frame, result)
          //generateMidpointDebugGeo(geodir, "bundler-midpoints", frame, result)

          //println("------- PASS 1 ---------")
          //println("SpringConst=%.6f ElectroConst=%.6f Radius=%.6f MinCompat=%.6f Converge=%.6f Step=[%.6f,%.6f]"
          // .format(springConst, electroConst, radius, minCompat, converge, step.lower, step.upper))

          for (i <- 0 until iterations) {
            result.iterate(springConst, electroConst, radius, minCompat, Some(attrs), converge, step)
            //generateSentimentGeo(geodir, "bundler-iter." + icnt, frame, result, attrs, densityRadius)
            icnt = icnt + 1
          }

          var it = iterations
          var rd = radius
          var cv = converge
          var sp = step
          for (p <- 2 to passes) {
            it = (it * 2) / 3
            rd = rd * 0.5
            sp = Interval(sp.lower, sp.upper * 0.5)

            println("Subdivide")
            result = result.subdivide

            println("------- PASS " + p + " ---------")
            println("SpringConst=%.6f ElectroConst=%.6f Radius=%.6f MinCompat=%.6f Converge=%.6f Step=[%.6f,%.6f]"
              .format(springConst, electroConst, rd, minCompat, cv, sp.lower, sp.upper))
            for (_ <- 0 until it) {
              result.iterate(springConst, electroConst, rd, minCompat, Some(attrs), cv, sp)
              generateSentimentGeo(geodir, "bundler-iter." + icnt, frame, result, attrs, densityRadius)
              icnt = icnt + 1
            }
          }

          generateSentimentGeo(geodir, "bundler-" + attrName, frame, result, attrs, densityRadius)
          println
        }
      }

      println
      println("ALL DONE!")
    }
    catch {
      case ex =>
        println("Uncaught Exception: " + ex.getMessage + "\n" +
          "Stack Trace:\n" + ex.getStackTraceString)
    }
  }

  /** Read in edges and associated attributes. */
  def readSentimentXML(xmldir: Path, prefix: String, frame: Int): (ForceDirectedEdgeBundler, Array[(Double, Double)]) = {
    val path = xmldir + Path(prefix) + (prefix + ".%04d.xml".format(frame))
    println("Reading XML File: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val elems = XML.load(in)
      val bundler = ForceDirectedEdgeBundler.fromXML(elems)
      val attrs =
        (elems \\ "Attributes") match {
          case as =>
            for (a <- as \\ "Attr") yield {
              val ary = a.text.trim.split(' ')
              (ary.head.toDouble, ary.last.toDouble)
            }
        }

      (bundler, attrs.toArray)
    }
    finally {
      in.close
    }
  }

  /** Generate polygonal lines in Houdini GEO format for e-mail activity and sentiment between people.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param bundler The source edges.
    * @param attrs The attributes for each endpoint of th edge.
    */
  def generateSentimentGeo(outdir: Path,
                           prefix: String,
                           frame: Int,
                           bundler: ForceDirectedEdgeBundler,
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

      val numPts = (for (ei <- 0 until bundler.numEdges) yield bundler.edgeSegs(ei) + 1).reduce(_ + _)
      val geo = GeoWriter(numPts, bundler.numEdges, pointAttrs = pointAttrs, primAttrs = primAttrs)
      geo.writeHeader(out)

      var icnt = 0
      var idxs: List[List[Int]] = List()

      geo.writePointAttrs(out)
      for (ei <- 0 until bundler.numEdges) {
        val (s, r) = attrs(ei)
        val numSegs = bundler.edgeSegs(ei)
        var eidxs: List[Int] = List()
        for (pi <- 0 to numSegs) {
          val t = pi.toDouble / numSegs.toDouble
          geo.setPointAttr(mag, Scalar.lerp(s, r, t))
          geo.setPointAttr(density, bundler.vertexDensity(Index2i(ei, pi), densityRadius))
          geo.writePoint(out, bundler(Index2i(ei, pi)).toPos3d)
          eidxs = icnt :: eidxs
          icnt = icnt + 1
        }
        idxs = eidxs.reverse :: idxs
      }
      idxs = idxs.reverse

      geo.writePrimAttrs(out)
      for ((s, r) <- attrs) {
        geo.setPrimAttr(mag, s max r)
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
                                 bundler: ForceDirectedEdgeBundler) {

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
                               bundler: ForceDirectedEdgeBundler) {

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