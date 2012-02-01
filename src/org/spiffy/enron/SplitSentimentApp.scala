package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import collection.immutable.TreeSet
import scala.xml.{ PrettyPrinter, XML }
import scala.math.log

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader }

object SplitSentimentApp {

  /** An sortable attribute value and edge index pair. */
  class AttrIndex private (val attr: Double, val idx: Int)
    extends Ordered[AttrIndex] {
    /** Ordered in decreasing attribute value and increasing index. */
    def compare(that: AttrIndex): Int =
      (that.attr compare attr) match {
        case 0 => idx compare that.idx
        case c => c
      }
  }

  object AttrIndex {
    def apply(attr: Double, idx: Int) = new AttrIndex(attr, idx)
  }

  /** Attribute names. */
  val attrNames = Array("sent", "words", "litigious", "modalstrong", "modalweak", "negative", "positive", "uncertainty", "undefined")
  val nattrNames = Array("litigious", "modalstrong", "negative", "positive", "uncertainty")

  /** Top level method. */
  def main(args: Array[String]) {
    try {
      // The directory to write XML format files.
      val xmldir = Path("./data/xml/sentiment")

      // Prefix of input XML files
      val prefix = "sentimentLinks"

      // Selected attribute indices to original indices.
      val idxMap = Array(2, 3, 5, 6, 7)

      println
      for (frame <- 1200 until 4200) {
        println
        println("---- Frame " + frame + "------------------------------------------")
        val (bundler, sentiment) = readSentimentXML(xmldir, prefix, frame)

        // Normalize the 5 sentiments we care about (replacing the existing 9 values).
        for (i <- 0 until sentiment.size) {
          sentiment(i) match {
            case (sa, ra) => {
              val (sao, rao) = (idxMap.map(sa(_)), idxMap.map(ra(_)))
              val (stl, rtl) = (sao.reduce(_ + _), rao.reduce(_ + _))
              val ns = if (stl > 0.0) sao.map(_ / stl) else Array.fill(sao.size)(0.0)
              val nr = if (rtl > 0.0) rao.map(_ / rtl) else Array.fill(rao.size)(0.0)
              sentiment(i) = (ns, nr)
            }
          }
        }

        // Export each sentiment separately. 
        for (nai <- 0 until nattrNames.size) {

          // Sort the sentiment values (with indices into the original values).
          var attrs = new TreeSet[AttrIndex]()
          for (ei <- 0 until bundler.numEdges) {
            sentiment(ei) match {
              case (sa, ra) => attrs = attrs + AttrIndex(sa(nai) max ra(nai), ei)
            }
          }

          // Get the top 100 values and a scaling factor for use when converting to log space.
          val topAttrs = attrs.toList.take(100).filter(_.attr > 0.0)
          val leastAttr = (attrs.head.attr /: attrs.tail) {
            case (mn, adx) => if (adx.attr > 0.0) mn min adx.attr else mn
          }
          
          // Scale and convert to log space so that the minimum value gets mapped to zero.
          //def toLog(d: Double) = if (d > 0.0) log(d / leastAttr) else 0.0

          // Print debug statistics.
          //val avg = topAttrs.map(ai => toLog(ai.attr)).reduce(_ + _) / topAttrs.size.toDouble
          //println("Attribute [" + nattrNames(nai) + "]")
          //println("-- Min=%.6f  Max=%.6f  Avg=%.6f  Size=%d"
          //  .format(toLog(topAttrs.last.attr), toLog(topAttrs.head.attr), avg, topAttrs.size))

          // Create a single sentiment bundler.
          val sbundler = ForceDirectedEdgeBundler(topAttrs.size)
          val sattrs = Array.fill(sbundler.numEdges)((0.0, 0.0))
          for ((adx, ei) <- topAttrs.zipWithIndex) {
            sbundler.resizeEdge(ei, 1)
            sbundler(Index2i(ei, 0)) = bundler(Index2i(adx.idx, 0))
            sbundler(Index2i(ei, 1)) = bundler(Index2i(adx.idx, bundler.edgeSegs(adx.idx))) 
            sattrs(ei) = sentiment(adx.idx) match {
              case (sa, ra) => (sa(nai), ra(nai))
            }
          }

          writeSentimentXML(xmldir, nai, frame, sbundler, sattrs)
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

  /** */
  def readSentimentXML(xmldir: Path,
                       prefix: String,
                       frame: Int): (ForceDirectedEdgeBundler, Array[(Array[Double], Array[Double])]) = {
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

  /** */
  def writeSentimentXML(xmldir: Path,
                        attrIdx: Int,
                        frame: Int,
                        bundler: ForceDirectedEdgeBundler,
                        sentiment: Array[(Double, Double)]) = {
    val path = xmldir + Path(nattrNames(attrIdx)) + (nattrNames(attrIdx) + ".%04d.xml".format(frame))
    println("Writing XML File: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val xml = <Links>{ bundler.toXML }<Attributes>{
        sentiment.map { case (s, r) => <Attr>{ "%.6f %.6f".format(s, r) }</Attr> }
      }</Attributes></Links>
      XML.write(out, xml, "UTF-8", false, null)
    }
    finally {
      out.close
    }
  }

}