package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Index2i, Scalar }
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

      // (iterations, converge, radius).
      val passes = List((50, 0.05, 0.25), (30, 0.025, 0.125))

      val springConst = 5.0
      val electroConst = 0.02
      val limits = (0.00001, 0.01)

      // The number of frames to process.
      val numFrames = 4621

      println
      var iter = 0
      for (frame <- 1800 until 1801) {
        val (bundler, sentiment) = readSentimentXML(xmldir, prefix, frame)
        println("Solving...")

        var result = subdivPrePass(bundler)
        //flattenPrePass(result)
        result.prepare
        generateSentimentGeo(geodir, "bundler-prepass", frame, result, sentiment)
        generatePrimaryDirDebugGeo(geodir, "bundler-primdirs", frame, result)
        generateMidpointDebugGeo(geodir, "bundler-midpoints", frame, result)

        val (in, ic, ir) = passes.head
        for (i <- 0 until in) {
          result.iterate(springConst, electroConst, ir, ic, limits)
          generateSentimentGeo(geodir, "bundler-iter." + iter, frame, result, sentiment)
          iter = iter + 1
        }

        for (((n, c, r), pi) <- passes.drop(1).zipWithIndex) {
          result = result.subdivide
          for (i <- 0 until n) {
            result.iterate(springConst, electroConst, r, c, limits)
            generateSentimentGeo(geodir, "bundler-iter." + iter, frame, result, sentiment)
            iter = iter + 1
          }
        }

        println
      }

      /*
      println
      for (frame <- 0 until numFrames) {
        val (bundler, sentiment) = readSentimentXML(xmldir, prefix, frame)
        println("Solving...")
        val result = subdivPrePass(bundler).solve(passes)
        generateSentimentGeo(geodir, prefix + "Bundled", frame, result, sentiment)
        println
      }
      */

      println
      println("ALL DONE!")
    }
    catch {
      case ex =>
        println("Uncaught Exception: " + ex.getMessage + "\n" +
          "Stack Trace:\n" + ex.getStackTraceString)
    }
  }

  /** */
  def readSentimentXML(xmldir: Path, prefix: String, frame: Int): (ForceDirectedEdgeBundler, Array[(Array[Double], Array[Double])]) = {
    val path = xmldir + (prefix + ".%04d.xml".format(frame))
    println("Reading XML File: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val elems = XML.load(in)
      val bundler = ForceDirectedEdgeBundler.fromXML(elems)
      val sentiment =
        for (bisnt <- (elems \\ "Attributes" \\ "AvgBiSent")) yield {
          val attrs =
            for (as <- bisnt \\ "AvgSent") yield {
              (as \ "@sent", as \ "@words") match {
                case (s, w) => Array(s.text.toDouble, w.text.toDouble) ++ as.text.trim.split(' ').map(_.toDouble)
              }
            }
          attrs match { case Seq(a, b) => (a, b) }
        }
      (bundler, sentiment.toArray)
    }
    finally {
      in.close
    }
  }

  def subdivPrePass(bundler: ForceDirectedEdgeBundler): ForceDirectedEdgeBundler = {
    if (bundler.numSegs != 3)
      throw new IllegalArgumentException("Only three segment initial edges are supported!")
    else {
      val sbundler = ForceDirectedEdgeBundler(bundler.numEdges, 6)

      for (ei <- 0 until bundler.numEdges) {
        val List(p0, p1, p2, p3) =
          (for (pi <- 0 to bundler.numSegs) yield { bundler(Index2i(ei, pi)) }).toList

        val s3 = Pos2d.lerp(p1, p2, 0.5)
        val (s1, s5) = (Pos2d.lerp(p0, p1, 0.5), Pos2d.lerp(p2, p3, 0.5))
        val (m2, m4) = (Pos2d.lerp(s1, s3, 0.5), Pos2d.lerp(s3, s5, 0.5))
        val spts = List(p0, s1, Pos2d.lerp(m2, p1, 0.5), s3, Pos2d.lerp(m4, p2, 0.5), s5, p3)

        for ((s, si) <- spts.zipWithIndex) 
          sbundler(Index2i(ei, si)) = Pos2d.lerp(s, s.toVec2d.normalized.toPos2d, 0.25)
      }

      sbundler
    }
  }

  def flattenPrePass(bundler: ForceDirectedEdgeBundler) = {
    for (ei <- 0 until bundler.numEdges) {
      val a = bundler(Index2i(ei, 0))
      val b = bundler(Index2i(ei, bundler.numSegs))
      for (i <- 1 until bundler.numSegs) {
        bundler(Index2i(ei, i)) = Pos2d.lerp(a, b, i.toDouble / bundler.numSegs.toDouble)
      }
    }
  }

  /** Generate polygonal lines in Houdini GEO format for e-mail activity and sentiment between people.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param bundler The source edges.
    * @param sentimentAttrs The source sentiment attributes.
    */
  def generateSentimentGeo(outdir: Path,
                           prefix: String,
                           frame: Int,
                           bundler: ForceDirectedEdgeBundler,
                           sentimentAttrs: Array[(Array[Double], Array[Double])]) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("Writing GEO File: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val names = List(
        "sent", "words",
        "litigious", "modalstrong", "modalweak", "negative", "positive", "uncertainty", "undefined"
      )

      val pointAttrs = names.map(PointFloatAttr(_, 0.0))
      val primAttrs = names.map(PrimitiveFloatAttr(_, 0.0))

      val numPts = bundler.numEdges * (bundler.numSegs + 1)
      val geo = GeoWriter(numPts, bundler.numEdges, pointAttrs = pointAttrs, primAttrs = primAttrs)
      geo.writeHeader(out)

      geo.writePointAttrs(out)
      for (((s, r), ei) <- sentimentAttrs.zipWithIndex) {
        for (pi <- 0 to bundler.numSegs) {
          val t = pi.toDouble / bundler.numSegs.toDouble
          for ((name, i) <- names.zipWithIndex)
            geo.setPointAttr(name, Scalar.lerp(s(i), r(i), t))
          geo.writePoint(out, bundler(Index2i(ei, pi)).toPos3d)
        }
      }

      geo.writePrimAttrs(out)
      for (((s, r), ei) <- sentimentAttrs.zipWithIndex) {
        val mx = for ((sa, ra) <- s zip r) yield { sa max ra }
        for ((name, i) <- names.zipWithIndex)
          geo.setPrimAttr(name, mx(i))
        val idxs = for (pi <- 0 to bundler.numSegs) yield { ei * (bundler.numSegs + 1) + pi }
        geo.writePolyLine(out, idxs.toList)
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
        val a = bundler(Index2i(ei, 0))
        val b = bundler(Index2i(ei, bundler.numSegs))
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