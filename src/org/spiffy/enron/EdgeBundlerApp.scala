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

      // Get the maximum attribute values over all frames.
      println("Determining Maximum Sentiment...")
      val maxSentiment = {
        def f(v: (Double, Double)) = v match { case (a, b) => a max b }
        def g(v: (Array[Double], Array[Double])) = v match { case (r, s) => (r zip s).map(f) }
        def h(a: (Array[Double], Array[Double]), b: (Array[Double], Array[Double])) = (g(a), g(b))

        val all =
          for (frame <- 0 until numFrames) yield {
            val sentiment = readSentimentAttrsXML(xmldir, prefix, frame)
            (sentiment.reduce(h) match { case (a, b) => a zip b }).map(f)
          }
        
        all.reduce((a, b) => (a zip b).map(f))
      }
      println("  " + maxSentiment.map("%.6f".format(_)).reduce(_ + " " + _))

      println
      for (frame <- 0 until numFrames) {
        val (bundler, sentiment) = readSentimentXML(xmldir, prefix, frame)
        println("Solving...")
        val result = bundler.solve(passes)
        generateSentimentGeo(geodir, prefix + "Bundled", frame, result, sentiment, maxSentiment)
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
  def readTrafficXML(xmldir: Path, prefix: String, frame: Int): (ForceDirectedEdgeBundler, Array[(Double, Double)]) = {
    val path = xmldir + (prefix + ".%04d.xml".format(frame))
    println("Reading XML File: " + path)
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val elems = XML.load(in)
      val bundler = ForceDirectedEdgeBundler.fromXML(elems)
      val traffic =
        for (sr <- (elems \\ "Traffic" \\ "SendRecv")) yield {
          sr.text.trim.split(' ').map(_.toDouble) match {
            case Array(send, recv) => (send, recv)
          }
        }
      (bundler, traffic.toArray)
    }
    finally {
      in.close
    }
  }

  /** */
  def readSentimentAttrsXML(xmldir: Path, prefix: String, frame: Int): Array[(Array[Double], Array[Double])] = {
    val path = xmldir + (prefix + ".%04d.xml".format(frame))
    val in = new BufferedReader(new FileReader(path.toFile))
    try {
      val elems = XML.load(in)
      val sentiment =
        for (bisnt <- (elems \\ "AverageBiSentiment" \\ "Link")) yield {
          val ts = for (term <- bisnt \\ "Terms") yield { term.text.trim.split(' ').map(_.toDouble) }
          ts match { case Seq(a, b) => (a, b) }
        }
      sentiment.toArray
    }
    finally {
      in.close
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
        for (bisnt <- (elems \\ "AverageBiSentiment" \\ "Link")) yield {
          val ts = for (term <- bisnt \\ "Terms") yield { term.text.trim.split(' ').map(_.toDouble) }
          ts match { case Seq(a, b) => (a, b) }
        }
      (bundler, sentiment.toArray)
    }
    finally {
      in.close
    }
  }

  /** Generate polygonal lines in Houdini GEO format for traffic between people.
    * @param outdir Path to the directory where the GEO files are written.
    * @param prefix The GEO filename prefix.
    * @param frame The number of the frame to generate.
    * @param
    * @param
    */
  def generateTrafficGeo(outdir: Path,
                         prefix: String,
                         frame: Int,
                         bundler: ForceDirectedEdgeBundler,
                         trafficAttrs: Array[(Double, Double)]) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("Writing GEO File: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val traffic = "traffic"
      val numPts = bundler.numEdges * (bundler.numSegs + 1)
      val geo = GeoWriter(numPts, bundler.numEdges, pointAttrs = List(PointFloatAttr(traffic, 0.0)))
      geo.writeHeader(out)

      geo.writePointAttrs(out)
      for (((s, r), ei) <- trafficAttrs.zipWithIndex) {
        for (pi <- 0 to bundler.numSegs) {
          geo.setPointAttr(traffic, Scalar.lerp(s, r, pi.toDouble / bundler.numSegs.toDouble))
          geo.writePoint(out, bundler(Index2i(ei, pi)).toPos3d)
        }
      }

      for (ei <- 0 until bundler.numEdges) {
        val idxs = for (pi <- 0 to bundler.numSegs) yield { ei * bundler.numSegs + pi }
        geo.writePolyLine(out, idxs.toList)
      }

      geo.writeFooter(out)

    }
    finally {
      out.close
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
                           sentimentAttrs: Array[(Array[Double], Array[Double])], 
                           maxSentiment: Array[Double]) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("Writing GEO File: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val names = List("litigious", "modalstrong", "modalweak", "negative", "positive", "uncertainty", "undefined")
      val words = "words"

      val numPts = bundler.numEdges * (bundler.numSegs + 1)
      val geo = GeoWriter(numPts, bundler.numEdges,
        pointAttrs = PointFloatAttr(words, 0.0) :: names.map(PointFloatAttr(_, 0.0)),
        primAttrs = PrimitiveFloatAttr(words, 0.0) :: names.map(PrimitiveFloatAttr(_, 0.0)))
      geo.writeHeader(out)

      val maxWords = maxSentiment.reduce(_ + _)
      
      geo.writePointAttrs(out)
      for (((s, r), ei) <- sentimentAttrs.zipWithIndex) {
        val sn = (s zip maxSentiment).map { case (a, b) => if(b > 0.0) a / b else 0.0 }
        val rn = (r zip maxSentiment).map { case (a, b) => if(b > 0.0) a / b else 0.0 }
        val swn = s.reduce(_ + _) / maxWords
        val rwn = r.reduce(_ + _) / maxWords        
        for (pi <- 0 to bundler.numSegs) {
          val t = pi.toDouble / bundler.numSegs.toDouble
          geo.setPointAttr(words, Scalar.lerp(swn, rwn,  t))
          for ((name, i) <- names.zipWithIndex)
            geo.setPointAttr(name, Scalar.lerp(sn(i), rn(i), t))
          geo.writePoint(out, bundler(Index2i(ei, pi)).toPos3d)
        }
      }

      geo.writePrimAttrs(out)
      for (ei <- 0 until bundler.numEdges) {
        val (sa, ra) = sentimentAttrs(ei)
        val a = for ((s, r) <- sa zip ra) yield { s max r }
        geo.setPrimAttr(words, a.reduce(_ + _) / maxWords)
        for ((name, i) <- names.zipWithIndex)
          geo.setPrimAttr(name, if(maxSentiment(i) > 0.0) a(i) / maxSentiment(i) else 0.0)
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