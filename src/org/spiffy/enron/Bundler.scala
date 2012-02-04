package org.spiffy.enron

import org.scalagfx.math.{ Pos2d, Vec2d, Index2i, Scalar, Interval }

import scala.xml.Node
import scala.math.{ E, abs, min, max, pow, sqrt }
import collection.mutable.{ HashMap }

/** A solver for an algorithm that adjusts the positions of vertices in a collection of segmented edges
  * so that similar edges are bundled more closely together.  The edges typically represent the control
  * vertices of parametric curves such as Bezier or NURBS curves.  Based on the "Force-Directed Edge
  * Bundling for Graph Visualization" paper by Danny Holten and Jarke J. van Wijk (Eurographics /
  * IEEE-VGTC Symposium on Visualization 2009).
  * @constructor
  * @param numEdges The total number of edges that will be processed.
  * @param original The original base solver (None if this is the original).
  */
class Bundler private (val numEdges: Int, original: Option[Bundler]) {
  /** The edge vertices. */
  private val verts: Array[Array[Pos2d]] = Array.fill(numEdges)(null)

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

  /** Get the number of segments in a specific edge. */
  def edgeSegs(idx: Int): Int = verts(idx).size - 1

  /** (Re)set the number of segments for a specific edge. */
  def resizeEdge(idx: Int, numSegs: Int) {
    verts(idx) = Array.fill(numSegs + 1)(Pos2d(0.0))
  }

  /** The normalized direction of the edge near the given vertex. */
  def vertexDir(idx: Index2i): Vec2d = {
    val (ai, bi) = if (idx.y > 0) (idx + Index2i(0, -1), idx) else (idx, idx + Index2i(0, 1))
    (this(ai) - this(bi)).normalized
  }

  /** A measure of how many vertices reside near the location of the given vertex with similar edge direction. */
  def vertexDensity(idx: Index2i, densityRadius: Double): Double = {
    val (ei, vi) = (idx.x, idx.y)
    val p = this(idx)
    val d = vertexDir(idx)
    val drSq = densityRadius * densityRadius
    val ed =
      for (oei <- 0 until numEdges; if (oei != ei)) yield {
        val vd =
          for (ovi <- 0 to edgeSegs(oei); if (ovi != vi)) yield {
            val lenSq = (p - this(Index2i(oei, ovi))).lengthSq
            if (lenSq > drSq) 0.0
            else {
              val wt = (d dot vertexDir(idx))
              wt * gauss(sqrt(lenSq), densityRadius)
            }
          }
        if (vd.isEmpty) 0.0 else vd.reduce(_ + _)
      }
    if (ed.isEmpty) 0.0 else ed.reduce(_ + _)
  }

  /** Normalized Gaussian function which decays to nearly zero at the given radius. */
  private def gauss(x: Double, radius: Double) = pow(E, -1.0 * pow((x / radius) * 2.0, 2.0))

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

  /** Compute the visibility compatibility measure [0,1] between two edges.
    * @param ai Index of edge A.
    * @param bi Index of edge B.
    */
  def visibilityCompat(ai: Int, bi: Int): Double = {
    val (a0, b0) = (this(Index2i(ai, 0)), this(Index2i(bi, 0)))
    val (a1, b1) = (this(Index2i(ai, edgeSegs(ai))), this(Index2i(bi, edgeSegs(bi))))
    val (am, bm) = (edgeMid(ai), edgeMid(bi))
    val (ad, bd) = (edgeDir(ai), edgeDir(bi))

    def vis(p0: Pos2d, p1: Pos2d, pm: Pos2d, pdir: Vec2d, q0: Pos2d, q1: Pos2d): Double = {
      def project(q: Pos2d): Pos2d = {
        val pq = (q - p0)
        val len = pq.length
        if (len < 1E-8) p0
        else p0 + (pdir * (pq dot pdir))
      }
      val l0 = project(q0)
      val l1 = project(q1)
      val lm = Pos2d.lerp(l0, l1, 0.5)
      max(0.0, 1.0 - ((2.0 * (pm - lm).length) / ((l0 - l1).length)))
    }

    val av = vis(a0, a1, am, ad, b0, b1)
    val bv = vis(b0, b1, bm, bd, a0, a1)
    min(av, bv)
  }

  /** Compute the total edge compatibility measure [0,1] between two edges.
    * @param ai Index of edge A.
    * @param bi Index of edge B.
    */
  def totalCompat(ai: Int, bi: Int): Double =
    angleCompat(ai, bi) * lengthCompat(ai, bi) * positionCompat(ai, bi) * visibilityCompat(ai, bi)

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
    * @param radius The distance at which the electrostatic force drops to zero.
    * @param minCompat The minimum total compatibility between edges for there to be electrostatic attraction.
    * @param weights Optional electrostatic force weights for each edge end point, defaults to 1.0 if not specified.
    * @param converge The speed with which the solution converges: a small number less than one.
    * @param step Limits on the distance a point may move in one iteration. Lower: The threshold below which a point will
    * not be moved at all.  Upper: The maximum amount a point will be moved.
    */
  def iterate(springConst: Double,
              electroConst: Double,
              radius: Double,
              minCompat: Double,
              weights: Option[Array[(Double, Double)]],
              converge: Double,
              step: Interval[Double]) {
    // Two less vertices per edge than edge positions, since end points don't move.
    val forces: Array[Array[Vec2d]] = Array.fill(numEdges)(null)

    // Compute forces acting on each vertex.
    for (ei <- 0 until numEdges) {
      val numSegs = edgeSegs(ei)
      val fs = Array.fill(numSegs - 1)(Vec2d(0.0))
      forces(ei) = fs

      // Spring forces between consecutive vertices.
      val k = springConst // / (edgeLength(ei) / numSegs.toDouble)
      for (vi <- 1 until numSegs) {
        val vm = this(Index2i(ei, vi - 1))
        val v = this(Index2i(ei, vi))
        val vp = this(Index2i(ei, vi + 1))
        fs(vi - 1) = ((vm - v) + (vp - v)) * k
      }

      // Electrostatic forced between all vertices of each edge scaled by compatibility measure.
      for (oei <- 0 until numEdges; if (ei != oei)) {
        val compat = totalCompat(ei, oei)
        val (wtA, wtB) = weights match {
          case Some(wts) => wts(oei)
          case None      => (1.0, 1.0)
        }
        if (compat > minCompat) {
          for (vi <- 1 until numSegs) {
            val v = this(Index2i(ei, vi))
            val onumSegs = edgeSegs(oei)
            for (ovi <- 1 until onumSegs) {
              val ov = this(Index2i(oei, ovi))
              val vec = ov - v
              val dist = vec.length
              if ((step.lower < dist) && (dist < radius)) {
                val eforce = gauss(dist, radius) * electroConst
                val wt = Scalar.lerp(wtA, wtB, ovi.toDouble / onumSegs.toDouble)
                val delta = ((vec / dist) * eforce * compat * wt)
                fs(vi - 1) = fs(vi - 1) + delta
              }
            }
          }
        }
      }
    }

    /** Move the vertices. */
    for (ei <- 0 until numEdges) {
      val numSegs = edgeSegs(ei)
      for (vi <- 1 until numSegs) {
        val cforce = forces(ei)(vi - 1) * converge
        val dist = cforce.length
        val delta =
          if (step.isBelow(dist)) Vec2d(0.0)
          else if (step.isAbove(dist)) (cforce / dist) * step.upper
          else cforce
        this(Index2i(ei, vi)) = this(Index2i(ei, vi)) + delta
      }
    }
  }

  /** Create a new solver in which all existing edges have been subdivided introducing a new vertex at
    * the midpoint of all existing edge segments.
    */
  def subdivide: Bundler = {
    val rtn = new Bundler(numEdges, Some(original.getOrElse(this)))
    for (ei <- 0 until numEdges) {
      val numSegs = edgeSegs(ei)
      rtn.resizeEdge(ei, numSegs * 2)
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

  /** Create a new solver in which the interior vertices of each edge are recreated by linearly interpolating the
    * positions of its end points at regular intervals so that the resulting segments are no greater than the given
    * length.
    */
  def retesselate(maxSegLength: Double): Bundler = {
    val rtn = new Bundler(numEdges, original)
    import scala.math.{ max, floor }
    for (ei <- 0 until numEdges) {
      val a = this(Index2i(ei, 0))
      val b = this(Index2i(ei, edgeSegs(ei)))
      val numSegs = max(2, scala.math.floor((b - a).length / maxSegLength).toInt)
      rtn.resizeEdge(ei, numSegs)
      rtn(Index2i(ei, 0)) = a
      for (i <- 1 until numSegs)
        rtn(Index2i(ei, i)) = Pos2d.lerp(a, b, i.toDouble / numSegs.toDouble)
      rtn(Index2i(ei, numSegs)) = b
    }
    rtn
  }

  /** Convert to an XML representation. */
  def toXML = {
    <Bundler numEdges={ numEdges.toString }>{
      verts.map(e => <Edge>{ e.map(p => <Pos2d>{ "%.8f %.8f".format(p.x, p.y) }</Pos2d>) }</Edge>)
    }<Lengths>{
      original match {
        case None =>
        case _ =>
          for (ei <- 0 until numEdges) yield {
            <Length>{ "%.8f".format(edgeLength(ei)) }</Length>
          }
      }
    }</Lengths><Directions>{
      original match {
        case None =>
        case _ => for (ei <- 0 until numEdges) yield {
          <Dir>{
            val d = edgeDir(ei)
            "%.8f %.8f".format(d.x, d.y)
          }</Dir>
        }
      }
    }</Directions><MidPoints>{
      original match {
        case None =>
        case _ => for (ei <- 0 until numEdges) yield {
          <Dir>{
            val d = edgeMid(ei)
            "%.8f %.8f".format(d.x, d.y)
          }</Dir>
        }
      }
    }</MidPoints></Bundler>
  }
}

object Bundler {
  /** Create a new solver.
    * @param numEdges The total number of edges that will be processed.
    */
  def apply(numEdges: Int) = new Bundler(numEdges, None)

  /** Create a new bundler from XML data. */
  def fromXML(node: Node): Bundler = {
    val numEdges = (node \ "@numEdges").text.toInt
    val bundler = Bundler(numEdges)
    for ((e, ei) <- (node \\ "Edge").zipWithIndex) {
      val verts = e \\ "Pos2d"
      bundler.resizeEdge(ei, verts.size - 1)
      for ((vert, vi) <- verts.zipWithIndex) {
        val pts = vert.text.trim.split(' ')
        (pts.map(_.toDouble)) match {
          case Array(x, y) => bundler(Index2i(ei, vi)) = Pos2d(x, y)
        }
      }
    }
    bundler
  }
}
