package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.{ PrettyPrinter, XML }
import scala.collection.mutable.Queue
import scala.math.{ E, pow }

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

      // Whether to output per-iteration geometry.
      val debug = true
      
      // The size of the bundling time filter.
      val width = 4

      // Iteration controls.
      val iterations = 60
      val converge = 0.0025 //* (1.0 / width.toDouble)
      val springConst = 6.0
      val electroConst = 0.15
      val radius = 0.2
      val minCompat = 0.1
      val step = Interval(1E-8, 0.01)
      val densityRadius = 0.01

      // The scaling factor for attribute values for each time sample.
      val weights = {
        def f(x: Double) = pow(E, -1.0 * pow(x * 2.0, 2.0))
        (for (i <- -width to width) yield f(i.toDouble / (width+1).toDouble)).toArray
      }
      
      // The total number of time samples.
      val window = weights.size

      // The FIFO of time samples.
      val cache: Queue[(Bundler, Array[(Double, Double)])] = Queue()

      // Bundle it...
      for (attrName <- attrNames) {
        for (frame <- 2000 until 2090 by 1) {

          val (bs, as) = readSentimentXML(xmldir, attrName, frame)
          cache.enqueue((bs.retesselate(radius * 0.5), as))

          if (cache.size > window)
            cache.dequeue

          if (cache.size == window) {
            val totalEdges = cache.map { case (b, _) => b.numEdges }.reduce(_ + _)
            val merged = Bundler(totalEdges)

            // Merge all edges from the cache together into one bundler.
            val (firstResultEdge, attrs) = {
              println("Merging Edges... (" + totalEdges + " Total)")
              var fre = 0
              var nre = 0
              var mei = 0
              var as: Array[(Double, Double)] = null
              for (((b, a), i) <- cache.zipWithIndex) {
                if (i == width) {
                  fre = mei
                  as = a
                }
                for (ei <- 0 until b.numEdges) {
                  val numSegs = b.edgeSegs(ei)
                  merged.resizeEdge(mei, numSegs)
                  for (vi <- 0 to numSegs)
                    merged(Index2i(mei, vi)) = b(Index2i(ei, vi))
                  mei = mei + 1
                }
              }

              (fre, as)
            }

            // Merge all attribute values, scaling them by the time filter.
            val mergedAttrs = {
              println("Merging Attributes...")
              val ma = Array.fill(totalEdges)((0.0, 0.0))
              var mei = 0
              for (((_, a), i) <- cache.zipWithIndex) {
                val wt = weights(i)
                for (ei <- 0 until a.size) {
                  a(ei) match { case (s, r) => ma(mei) = (s * wt, r * wt) }
                  mei = mei + 1
                }
              }
              ma
            }

            // Bundle the merged edges together at one time.
            print("Bundling Edges: ")
            if(debug) println
            merged.prepare
            for (i <- 0 until iterations) {
              if(debug) generateSentimentGeo(geodir, "bundler-iter." + i + "." + attrName, frame-width, merged, 0, mergedAttrs, densityRadius)
              merged.iterate(springConst, electroConst, radius, minCompat, Some(mergedAttrs), converge, step)
              if(!debug) print(".")
            }
            if(!debug) println

            // Write out the GEO results.
            generateSentimentGeo(geodir, "bundler-" + attrName, frame-width, merged, firstResultEdge, attrs, densityRadius)
            println
          }
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
  def readSentimentXML(xmldir: Path, prefix: String, frame: Int): (Bundler, Array[(Double, Double)]) = {
    val path = xmldir + Path(prefix) + (prefix + ".%04d.xml".format(frame))
    println("Reading XML File: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val elems = XML.load(in)
      val bundler = Bundler.fromXML(elems)
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
                           bundler: Bundler,
                           firstEdge: Int, 
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
      for (ei <- 0 until attrs.size) {
        val (s, r) = attrs(ei)
        val numSegs = bundler.edgeSegs(ei+firstEdge)
        var eidxs: List[Int] = List()
        for (pi <- 0 to numSegs) {
          val t = pi.toDouble / numSegs.toDouble
          geo.setPointAttr(mag, Scalar.lerp(s, r, t))
          val idx = Index2i(ei+firstEdge, pi)
          geo.setPointAttr(density, bundler.vertexDensity(idx, densityRadius))
          geo.writePoint(out, bundler(idx).toPos3d)
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