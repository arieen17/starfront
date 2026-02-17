package edu.ucr.cs.bdlab.ucrstar

import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, CoordinateXY, Geometry, GeometryFactory}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeometryBreakerTest extends FunSuite with ScalaSparkTest {

  val factory = new GeometryFactory()
  def createSequence(points: (Double, Double)*): CoordinateSequence = {
    val cs = factory.getCoordinateSequenceFactory.create(points.length, 2)
    for (i <- points.indices) {
      cs.setOrdinate(i, 0, points(i)._1)
      cs.setOrdinate(i, 1, points(i)._2)
    }
    cs
  }

  test("Break polygon") {
    val numPoints = 19
    val threshold = 10
    val coords = new Array[Coordinate](numPoints)
    for (i <- 0 until numPoints - 1) {
      val degree = Math.PI * 2 * i / (numPoints - 1)
      coords(i) = new CoordinateXY(Math.cos(degree), Math.sin(degree))
    }
    coords(numPoints - 1) = coords(0)
    val polygon = factory.createPolygon(coords)

    val expectedGeometries: Array[Geometry] = new Array[Geometry](2)
    val subcoords1 = new Array[Coordinate](threshold)
    for (i <- 0 until threshold) {
      val degree = Math.PI * 2 * i / (numPoints - 1)
      subcoords1(i) = new CoordinateXY(Math.cos(degree), Math.sin(degree))
    }
    expectedGeometries(0) = factory.createLineString(subcoords1)

    val subcoords2 = new Array[Coordinate](threshold)
    for (i <- threshold - 1 until numPoints) {
      val degree = Math.PI * 2 * i / (numPoints - 1)
      subcoords2(i - (threshold - 1)) = new CoordinateXY(Math.cos(degree), Math.sin(degree))
    }
    expectedGeometries(1) = factory.createLineString(subcoords2)

    val brokenGeometry = new GeometryBreaker(polygon, threshold)

    val actualGeometries = brokenGeometry.toArray
    assert(expectedGeometries.length == actualGeometries.length)
    for (i <- expectedGeometries.indices)
      assert(expectedGeometries(i).equalsExact(actualGeometries(i), 1E-3),
      s"Expected '${expectedGeometries(i)}' but found '${actualGeometries(i)}'")
  }
}
