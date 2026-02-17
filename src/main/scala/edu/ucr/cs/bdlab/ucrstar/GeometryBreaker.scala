package edu.ucr.cs.bdlab.ucrstar

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, PointND}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryCollection, LineString, Point, Polygon}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Breaks complex geometries into simpler ones that can be processed easily (for some queries).
 * It is specifically designed for visualization of the boundaries of the geometries.
 * @param geometry the geometry to break down
 * @param threshold the maximum number of points to keep per geometry
 */
class GeometryBreaker(geometry: Geometry, threshold: Int = 100) extends Iterator[Geometry] with java.util.Iterator[Geometry] {
  require(threshold >= 5, s"Threshold $threshold is too small. Minimum threshold is five")

  val toBreak: mutable.ListBuffer[Geometry] = new ListBuffer[Geometry]()
  if (!geometry.isEmpty)
    toBreak += geometry

  override def hasNext: Boolean = !toBreak.isEmpty

  override def next(): Geometry = {
    while (!toBreak.isEmpty) {
      val geom : Geometry = toBreak.remove(0)
      if (geom.getNumPoints <= threshold)
        return geom
      // A complex geometry should be broken down into simpler ones.
      geom match {
        case p: Point => return p
        case p: PointND => return p
        case e: EnvelopeND => return e
        case gc: GeometryCollection =>
          // GeometryCollection also covers MultiLineString, MultiPoint, and MultiPolygon
          for (i <- 0 until gc.getNumGeometries)
            if (!gc.getGeometryN(i).isEmpty)
              toBreak += gc.getGeometryN(i)
        case p: Polygon =>
          // Add all rings
          toBreak += p.getExteriorRing
          for (i <- 0 until p.getNumInteriorRing)
            toBreak += p.getInteriorRingN(i)
        case l: LineString =>
          var i1: Int = 0
          val coords: Array[Coordinate] = new Array[Coordinate](threshold)
          // Add all segments as separate line strings
          for (i <- 0 until l.getNumPoints) {
            if (i - i1 >= threshold) {
              // Accumulated enough points to create a geometry
              toBreak += geom.getFactory.createLineString(coords.slice(0, threshold))
              // Move the last point to be the first point to ensure connectivity between sub-geometries
              coords(0) = coords(threshold - 1)
              i1 = i - 1
            }
            coords(i - i1) = l.getCoordinateN(i)
            (s"Copying ${i}: ${l.getCoordinateN(i)}")
          }
          // Add the last part
          toBreak += geom.getFactory.createLineString(coords.slice(0, l.getNumPoints - i1))
        case _ => throw new RuntimeException(s"Unexpected geometry type '${geom.getGeometryType}''")
      }
    }
    throw new RuntimeException("Unexpected to reach this part of the code")
  }

}
