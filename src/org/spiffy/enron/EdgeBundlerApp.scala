package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Index2i, Scalar }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.XML

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

      // Number of iterations per pass.
      val passes = List(1)

      // The number of frames to process.
      val numFrames = 731

      println
      for (frame <- 0 until numFrames) {
        val (bundler, sentiment) = readSentimentXML(xmldir, prefix, frame)
        println("Solving...")
        val result = bundler.solve(passes)
        generateSentimentGeo(geodir, prefix + "Bundled", frame, result, sentiment)
        println
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

  /** Generate polygonal lines in Houdini GEO format for e-mail activity and sentiment between people.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param
    * @param
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

}