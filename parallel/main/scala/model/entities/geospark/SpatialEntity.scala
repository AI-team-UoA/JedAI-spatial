package model.entities.geospark

import com.vividsolutions.jts.geom.Geometry

case class SpatialEntity(originalID: String, geometry: Geometry) extends Entity

