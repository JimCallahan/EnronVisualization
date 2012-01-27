package org.spiffy.enron

import org.scalagfx.math.{ Pos2d, Vec2d, Index2i }

import scala.xml.Elem

/** A solver for an algorithm that adjusts the positions of vertices in a collection of segmented edges
  * so that similar edges are bundled more closely together.  The edges typically represent the control
  * vertices of parametric curves such as Bezier or NURBS curves.  Based on the "Force-Directed Edge
  * Bundling for Graph Visualization" paper by Danny Holten and Jarke J. van Wijk (Eurographics /
  * IEEE-VGTC Symposium on Visualization 2009).
  * @constructor
  * @param numEdges The total number of edges that will be processed.
  * @param numSegs The number of segments (at least 2) in each edge.
  */
class ForceDirectedEdgeBundler private (val numEdges: Int, val numSegs: Int) {
  /** Internal storage for the edge vertices. */
  private val verts = Array.fill(numEdges)(Array.fill(numSegs + 1)(Pos2d(0.0)))

  /** Lookup the position of an edge vertex.
    * @param idx The (edge, vertex) index.
    */
  def apply(idx: Index2i): Pos2d =
    verts(idx.x)(idx.y)

  /** Update the position of an edge vertex.
    * @param edgeIdx The edge index.
    * @param vertIdx The vertex index.
    * @param p The new position.
    */
  def update(idx: Index2i, p: Pos2d) {
    verts(idx.x)(idx.y) = p
  }

  /** Compute the solution in several passes, each consisting of a number of iterations followed by
    * a subdivision before starting the next pass.
    * @param passes The number of iterations in each pass.
    * @return The edge results.
    */
  def solve(passes: List[Int]): ForceDirectedEdgeBundler = {
    var bundler = this
    for (_ <- 0 until passes.head) bundler.iterate
    for (p <- passes.drop(1)) {
      bundler = bundler.subdivide
      for (_ <- 0 until p) bundler.iterate
    }
    bundler
  }

  /** Perform one iteration step of the algorithm. */
  def iterate() {

  }

  /** Create a new solver in which all existing edges have been subdivided introducing a new vertex at
    * the midpoint of all existing edge segments.
    */
  def subdivide(): ForceDirectedEdgeBundler = {
    val rtn = ForceDirectedEdgeBundler(numEdges, numSegs * 2)
    for (i <- 0 until numEdges) {
      for (j <- 0 until numSegs) {
        val a = this(Index2i(i, j))
        val b = this(Index2i(i, j + 1))
        rtn(Index2i(i, j * 2)) = a
        rtn(Index2i(i, j * 2 + 1)) = Pos2d.lerp(a, b, 0.5)
      }
      rtn(Index2i(i, numSegs)) = this(Index2i(i, numSegs))
    }
    rtn
  }

  /** Convert to an XML representation. */
  def toXML: Elem = {
    <ForceDirectedEdgeBundler numEdges={ numEdges.toString } numSegs={ numSegs.toString }>{
      verts.map(e => <Edge>{ e.map(p => <Pos2d>{ "%.6f %.6f".format(p.x, p.y) }</Pos2d>) }</Edge>)
    }</ForceDirectedEdgeBundler>
  }
}

object ForceDirectedEdgeBundler {
  /** Create a new solver.
    * @param numEdges The total number of edges that will be processed.
    * @param numSegs The number of segments (at least 2) in each edge.
    */
  def apply(numEdges: Int, numSegs: Int) = new ForceDirectedEdgeBundler(numEdges, numSegs)

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
