package org.spiffy.enron

import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Scalar }

import scala.xml.Elem
import scala.math.{ E, abs, min, max, pow }
import collection.mutable.{ HashMap }

/** A solver for an algorithm that adjusts the positions of vertices in a collection of segmented edges
  * so that similar edges are bundled more closely together.  The edges typically represent the control
  * vertices of parametric curves such as Bezier or NURBS curves.  Based on the "Force-Directed Edge
  * Bundling for Graph Visualization" paper by Danny Holten and Jarke J. van Wijk (Eurographics /
  * IEEE-VGTC Symposium on Visualization 2009).
  * @constructor
  * @param numEdges The total number of edges that will be processed.
  * @param numSegs The number of segments (at least 2) in each edge.
  * @param original The original base solver (None if this is the original).
  */
class ForceDirectedEdgeBundler private (val numEdges: Int,
                                        val numSegs: Int,
                                        original: Option[ForceDirectedEdgeBundler]) {
  /** The edge vertices. */
  private val verts = Array.fill(numEdges)(Array.fill(numSegs + 1)(Pos2d(0.0)))

  /** Lookup the position of an edge vertex.
    * @param idx The (edge, vertex) index.
    */
  def apply(idx: Index2i): Pos2d =
    verts(idx.x)(idx.y)

  /** Update the position of an edge vertex.
    * @param idx The (edge, vertex) index.
    * @param p The new position.
    */
  def update(idx: Index2i, p: Pos2d) {
    verts(idx.x)(idx.y) = p
  }

  //-----------------------------------------------------------------------------------------------------------------------------------

  /** The total length of the original edge. */
  private var edgeLengths: Option[Array[Double]] = None

  /** The normalized direction from the first to last vertex of each edge. */
  private var edgeDirs: Option[Array[Vec2d]] = None

  /** The position of the midpoint of the original curve. */
  private var edgeMids: Option[Array[Pos2d]] = None

  /** The length of the original edge.
    * @param idx The edge index.
    */
  def edgeLength(idx: Int): Double = {
    original match {
      case Some(orig) => orig.edgeLength(idx)
      case _ =>
        edgeLengths match {
          case Some(ls) => ls(idx)
          case _ =>
            throw new IllegalArgumentException("The lengths have not been computed yet!")
        }
    }
  }

  /** The normalized direction from the first to last vertex of the original edge.
    * @param idx The edge index.
    */
  def edgeDir(idx: Int): Vec2d = {
    original match {
      case Some(orig) => orig.edgeDir(idx)
      case _ =>
        edgeDirs match {
          case Some(ds) => ds(idx)
          case _ =>
            throw new IllegalArgumentException("The edge directions have not been computed yet!")
        }
    }
  }

  /** The position of the midpoint of the original curve.
    * @param idx The edge index.
    */
  def edgeMid(idx: Int): Pos2d = {
    original match {
      case Some(orig) => orig.edgeMid(idx)
      case _ =>
        edgeMids match {
          case Some(ms) => ms(idx)
          case _ =>
            throw new IllegalArgumentException("The edge midpoints have not been computed yet!")
        }
    }
  }

  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Compute the angle compatibility measure [0,1] between two edges.
    * Edges with equal or opposite directions will have compatibility of one, perpendicular edges zero.
    * @param ai Index of edge A.
    * @param bi Index of edge B.
    */
  def angleCompat(ai: Int, bi: Int): Double =
    abs(edgeDir(ai) dot edgeDir(bi))

  /** Compute the length compatibility measure [0,1] between two edges.
    * Edges with equal length will have compatibility of one, while approaching zero for higher ratios of length.
    * @param ai Index of edge A.
    * @param bi Index of edge B.
    */
  def lengthCompat(ai: Int, bi: Int): Double = {
    val (a, b) = (edgeLength(ai), edgeLength(bi))
    val lavg = Scalar.lerp(a, b, 0.5)
    2.0 / (lavg * min(a, b) + max(a, b) / lavg)
  }

  /** Compute the position compatibility measure [0,1] between two edges.
    * Edges with coinciding midpoints will have compatibility of one, while approaching zero at infinite distance.
    * @param ai Index of edge A.
    * @param bi Index of edge B.
    */
  def positionCompat(ai: Int, bi: Int): Double = {
    val (a, b) = (edgeMid(ai), edgeMid(bi))
    val lavg = Scalar.lerp(edgeLength(ai), edgeLength(bi), 0.5)
    lavg / (lavg + (a - b).length)
  }

  /** Compute the total edge compatibility measure [0,1] between two edges.
    * @param ai Index of edge A.
    * @param bi Index of edge B.
    */
  def totalCompat(ai: Int, bi: Int): Double =
    angleCompat(ai, bi) * lengthCompat(ai, bi) * positionCompat(ai, bi)

  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Prepare the solver by computing the edge lengths, directions and compatibility between the
    * original edges which will be shared by all subsequent solvers.
    */
  def prepare() {
    original match {
      case None => {
        val ls =
          for (vs <- verts) yield {
            (for (pair <- vs.sliding(2)) yield {
              pair match { case Array(a, b) => (b - a).length }
            }).reduce(_ + _)
          }
        edgeLengths = Some(ls)

        edgeDirs = Some(for (vs <- verts) yield (vs.last - vs.head).normalized)
        edgeMids = Some(for (vs <- verts) yield Pos2d.lerp(vs.last, vs.head, 0.5))
      }
      case _ =>
        throw new IllegalStateException("Only the original solver should be prepared!")
    }
  }

  //-----------------------------------------------------------------------------------------------------------------------------------

  /** Perform one iteration step of the algorithm.
    * @param springConst The strength of the spring constant between consecutive vertices of an edge.
    * @param electroConst The strength of the electrostatic force between corresponding vertices of pairs of edges.
    * @param radius The radius at which the electrostatic force drops to zero.
    * @param converge The speed with which the solution converges: a small number less than one.
    * @param limits The (minmum, maximum) distance a point may move in one iteration.
    */
  def iterate(springConst: Double, electroConst: Double, radius: Double, converge: Double, limits: (Double, Double)) {
    // Two less vertices per edge than edge positions, since end points don't move.
    val forces = Array.fill(numEdges)(Array.fill(numSegs - 1)(Vec2d(0.0)))

    // Compute forces acting on each vertex.
    for (ei <- 0 until numEdges) {
      // Spring forces between consecutive vertices.
      val k = springConst / (edgeLength(ei) * numSegs.toDouble)
      for (vi <- 1 until numSegs) {
        val vm = this(Index2i(ei, vi - 1))
        val v = this(Index2i(ei, vi))
        val vp = this(Index2i(ei, vi + 1))
        val sforce = ((vm - v) + (vp - v)) * k
        forces(ei)(vi - 1) = sforce
      }

      // Electrostatic forced between corresponding vertices of each edge scaled by compatibility measure.
      def gauss(x: Double, r: Double) = pow(E, -1.0 * pow((x / r) * 2.0, 2.0))
      for (oei <- 0 until numEdges; if (ei != oei)) {
        val compat = totalCompat(ei, oei)
        for (vi <- 1 until numSegs) {
          val v = this(Index2i(ei, vi))
          for (ovi <- 1 until numSegs) {
            val vo = this(Index2i(oei, ovi))
            val v2o = vo - v
            val len = v2o.length
            val eforce = if (len < 1E-6) Vec2d(0.0) else v2o * ((electroConst * gauss(len, radius)) / len)
            forces(ei)(vi - 1) = forces(ei)(vi - 1) + eforce * compat
          }
        }
      }
    }

    /** Move the vertices. */
    val (mind, maxd) = limits
    for (ei <- 0 until numEdges) {
      for (vi <- 1 until numSegs) {
        val delta = forces(ei)(vi - 1) * converge
        val mag = delta.length
        val step =
          if (mag < mind) Vec2d(0.0)
          else if (mag > maxd) delta * (maxd / mag)
          else delta
          
        this(Index2i(ei, vi)) = this(Index2i(ei, vi)) + step
      }
    }
  }

  /** Create a new solver in which all existing edges have been subdivided introducing a new vertex at
    * the midpoint of all existing edge segments.
    */
  def subdivide: ForceDirectedEdgeBundler = {
    val rtn = new ForceDirectedEdgeBundler(numEdges, numSegs * 2, Some(original.getOrElse(this)))
    for (ei <- 0 until numEdges) {
      for (pi <- 0 until numSegs) {
        val a = this(Index2i(ei, pi))
        val b = this(Index2i(ei, pi + 1))
        rtn(Index2i(ei, pi * 2)) = a
        rtn(Index2i(ei, pi * 2 + 1)) = Pos2d.lerp(a, b, 0.5)
      }
      rtn(Index2i(ei, numSegs * 2)) = this(Index2i(ei, numSegs))
    }
    rtn
  }

  /** Convert to an XML representation. */
  def toXML: Elem = {
    <ForceDirectedEdgeBundler numEdges={ numEdges.toString } numSegs={ numSegs.toString }>{
      verts.map(e => <Edge>{ e.map(p => <Pos2d>{ "%.6f %.6f".format(p.x, p.y) }</Pos2d>) }</Edge>)
    }<Lengths>{
      original match {
        case None =>
        case _ =>
          for (ei <- 0 until numEdges) yield {
            <Length>{ "%.6f".format(edgeLength(ei)) }</Length>
          }
      }
    }</Lengths><Directions>{
      original match {
        case None =>
        case _ => for (ei <- 0 until numEdges) yield {
          <Dir>{
            val d = edgeDir(ei)
            "%.6f %.6f".format(d.x, d.y)
          }</Dir>
        }
      }
    }</Directions><MidPoints>{
      original match {
        case None =>
        case _ => for (ei <- 0 until numEdges) yield {
          <Dir>{
            val d = edgeMid(ei)
            "%.6f %.6f".format(d.x, d.y)
          }</Dir>
        }
      }
    }</MidPoints></ForceDirectedEdgeBundler>
  }
}

object ForceDirectedEdgeBundler {
  /** Create a new solver.
    * @param numEdges The total number of edges that will be processed.
    * @param numSegs The number of segments (at least 2) in each edge.
    * @param springConst The strength of the spring constant between consecutive vertices of an edge.
    * @param electroConst The strength of the electrostatic force between corresponding vertices of pairs of edges.
    */
  def apply(numEdges: Int, numSegs: Int) = new ForceDirectedEdgeBundler(numEdges, numSegs, None)

  /** Create a new bundler from XML data. */
  def fromXML(elem: Elem): ForceDirectedEdgeBundler = {
    val b = elem \\ "ForceDirectedEdgeBundler"
    val (e, s) = (b \ "@numEdges", b \ "@numSegs")
    val bundler = ForceDirectedEdgeBundler(e.text.toInt, s.text.toInt)
    var eidx = 0
    for (e <- b \\ "Edge") {
      var pidx = 0
      for (p <- e \\ "Pos2d") {
        (p.text.trim.split(' ').map(_.toDouble)) match {
          case Array(x, y) => bundler(Index2i(eidx, pidx)) = Pos2d(x, y)
        }
        pidx = pidx + 1
      }
      eidx = eidx + 1
    }
    bundler
  }
}
