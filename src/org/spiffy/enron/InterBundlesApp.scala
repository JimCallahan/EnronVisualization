package org.spiffy.enron

import org.scalagfx.io.Path
import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Index3i, Frame2d, Scalar, Interval }
import org.scalagfx.houdini.geo.GeoWriter
import org.scalagfx.houdini.geo.attr.{ PointFloatAttr, PrimitiveFloatAttr }

import scala.xml.{ PrettyPrinter, XML }
import scala.collection.immutable.{ TreeSet, TreeMap }
import scala.collection.mutable.{ HashMap, HashSet }
import scala.math.{ E, pow, log }

import java.io.{ BufferedWriter, BufferedReader, FileWriter, FileReader, IOException }

import scala.xml.{ Node, XML }

object InterpBundlesApp
  extends CommonIO {

  class InterpBundler private (val bundlerA: Bundler,
                               val attrsA: Array[AttrIndex],
                               val bundlerB: Bundler,
                               val attrsB: Array[AttrIndex]) {

    private val indexA = HashMap[Long, HashMap[Long, Int]]()
    private val indexB = HashMap[Long, HashMap[Long, Int]]()

    def indexOf(sendID: Long, recvID: Long, index: HashMap[Long, HashMap[Long, Int]]): Option[Int] = {
      index.get(sendID) match {
        case None     => None
        case Some(rm) => rm.get(recvID)
      }
    }

    private val ids = {
      val peopleIDs = HashMap[Long, HashSet[Long]]()
      for ((attrs, index) <- List((attrsA, indexA), (attrsB, indexB))) {
        for ((ai, ei) <- attrs.zipWithIndex) {
          peopleIDs.getOrElseUpdate(ai.sendID, HashSet[Long]()) += ai.recvID
          index.getOrElseUpdate(ai.sendID, HashMap[Long, Int]()) += (ai.recvID -> ei)
        }
      }

      val size = peopleIDs.values.map(_.size).reduce(_ + _)
      val rtn = Array.fill(size)((0L, 0L))
      var c = 0
      for ((sid, rs) <- peopleIDs) {
        for (rid <- rs) {
          rtn(c) = (sid, rid)
          c = c + 1
        }
      }
      rtn
    }

    val numEdges = ids.size

    def edgeIDs(ei: Int) = ids(ei)

    def edgeSegs(ei: Int) =
      edgeIDs(ei) match {
        case (sid, rid) =>
          (indexOf(sid, rid, indexA), indexOf(sid, rid, indexB)) match {
            case (Some(ai), None) => bundlerA.edgeSegs(ai)
            case (None, Some(bi)) => bundlerB.edgeSegs(bi)
            case (Some(ai), Some(bi)) => {
              val as = bundlerA.edgeSegs(ai)
              val bs = bundlerB.edgeSegs(bi)
              if (as != bs) throw new IllegalStateException("Somehow the number of segments doesn't match!")
              else as
            }
            case _ => throw new IllegalStateException("Somehow the edge doesn't exist!")
          }
      }

    def interp(ei: Int, vi: Int, t: Double): Pos2d = {
      edgeIDs(ei) match {
        case (sid, rid) =>
          (indexOf(sid, rid, indexA), indexOf(sid, rid, indexB)) match {
            case (Some(ai), None) => bundlerA(Index2i(ai, vi))
            case (None, Some(bi)) => bundlerB(Index2i(bi, vi))
            case (Some(ai), Some(bi)) => {
              val a = bundlerA(Index2i(ai, vi))
              val b = bundlerB(Index2i(bi, vi))
              Pos2d.smoothlerp(a, b, t)
            }
            case _ => throw new IllegalStateException("Somehow the edge doesn't exist!")
          }
      }
    }
  }

  object InterpBundler {
    def apply(bundlerA: Bundler, attrsA: Array[AttrIndex], bundlerB: Bundler, attrsB: Array[AttrIndex]) =
      new InterpBundler(bundlerA, attrsA, bundlerB, attrsB)
  }

  /** Top level method. */
  def main(args: Array[String]) {
    try {
      // The directory to write Houdini GEO format files.
      val geodir = Path("./artwork/houdini/geo/interpBundles")

      // The directory to write Houdini HScript format files.
      val hsdir = Path("./artwork/houdini/hscript")

      // The directory from which to read XML format files.
      val xmldir = Path("./data/xml")

      // The financial terms we are interested in bundling. 
      import FinancialTerm._
      val bundleTerms = Array(Litigious, ModalStrong, Negative, Positive, Uncertainty)

      println("Loading People...")
      val centrality = readMostCentralXML
      val people = readPeopleXML(centrality)

      val ibundlers = HashMap[FinancialTerm.Value, InterpBundler]()
      for (frame <- 30 until 1530 by 1) {
        val (_, _, avgBiSent) = readBundleSentimentSamplesXML(xmldir, "averageBiSentimentSample", frame)
        for (term <- bundleTerms) {
          if (frame % 15 == 0) {
            val (bundlerA, attrsA) =
              readBundlerAttrsXML(xmldir + "bundledEdges", "bundledEdges-" + term, frame)
            val (bundlerB, attrsB) =
              readBundlerAttrsXML(xmldir + "bundledEdges", "bundledEdges-" + term, frame + 15)
            ibundlers += (term -> InterpBundler(bundlerA, attrsA, bundlerB, attrsB))
          }
          val ibundler = ibundlers(term)
          val t = (frame%15).toDouble / 15.0
          generateBundleGeo(geodir, "interpBundled-" + term, frame, t, term, bundleTerms, ibundler, avgBiSent)
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
    */
  def generateBundleGeo(outdir: Path,
                        prefix: String,
                        frame: Int,
                        bundleT: Double,
                        term: FinancialTerm.Value,
                        bundleTerms: Array[FinancialTerm.Value],
                        ibundler: InterpBundler,
                        bisent: HashMap[Long, HashMap[Long, AverageBiSentiment]]) {

    import scala.math.{ ceil, log, Pi }
    val path = outdir + (prefix + ".%04d.geo".format(frame))
    println("  Writing: " + path)
    val out = new BufferedWriter(new FileWriter(path.toFile))
    try {
      val mag = "mag"
      val pointAttrs = List(PointFloatAttr(mag, 0.0))
      val primAttrs = List(PrimitiveFloatAttr(mag, 0.0))

      val numPts = (for (ei <- 0 until ibundler.numEdges) yield ibundler.edgeSegs(ei) + 3).reduce(_ + _)
      val geo = GeoWriter(numPts, ibundler.numEdges, pointAttrs = pointAttrs, primAttrs = primAttrs)
      geo.writeHeader(out)

      var icnt = 0
      var idxs: List[List[Int]] = List()

      val shrink = 0.925

      def f(snt: AverageSentiment): Double = {
        val sum = bundleTerms.map(snt.freq(_)).reduce(_ + _)
        if(sum > 0.0) snt.freq(term) / sum else 0.0 
      }
      
      def attrs(ei: Int): (Double, Double) = {
        val (sid, rid) = ibundler.edgeIDs(ei)
        bisent.get(sid) match {
          case None => (0.0, 0.0)
          case Some(rm) =>
            rm.get(rid) match {
              case None     => (0.0, 0.0)
              case Some(bs) => (f(bs.send), f(bs.recv))
            }
        }
      }
      
      geo.writePointAttrs(out)
      for (ei <- 0 until ibundler.numEdges) {
        val (s, r) = attrs(ei)
        val numSegs = ibundler.edgeSegs(ei)
        var eidxs: List[Int] = List()
        for (ppi <- -1 to numSegs + 1) {
          val pi = Scalar.clamp(ppi, 0, numSegs)
          val t = pi.toDouble / numSegs.toDouble
          geo.setPointAttr(mag, Scalar.lerp(s, r, t))
          val pp = ibundler.interp(ei, pi, bundleT)
          val p = if ((ppi == -1) || (ppi == numSegs + 1)) pp else pp * shrink
          geo.writePoint(out, p.toPos3d)
          eidxs = icnt :: eidxs
          icnt = icnt + 1
        }
        idxs = eidxs.reverse :: idxs
      }
      idxs = idxs.reverse

      geo.writePrimAttrs(out)
      for (ei <- 0 until ibundler.numEdges) {
        val (s, r) = attrs(ei)
        geo.setPrimAttr(mag, s + r)
        geo.writePolyLine(out, idxs.head)
        idxs = idxs.drop(1)
      }

      geo.writeFooter(out)
    }
    finally {
      out.close
    }
  }

}