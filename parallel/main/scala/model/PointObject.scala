package model

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}

object PointObject {

  def apply(x: Int, y: Int): Point = {
    val gf: GeometryFactory = new GeometryFactory()
    gf.createPoint(new Coordinate(x, y))
  }

  def apply(x: Float, y: Float): Point = {
    val gf: GeometryFactory = new GeometryFactory()
    gf.createPoint(new Coordinate(x, y))
  }

  def apply(x: Double, y: Double): Point = {
    val gf: GeometryFactory = new GeometryFactory()
    gf.createPoint(new Coordinate(x, y))
  }

}
