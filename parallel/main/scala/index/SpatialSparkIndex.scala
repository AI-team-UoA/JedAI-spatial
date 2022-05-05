package index

import model.entities.Entity
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.index.strtree.STRtree

case class SpatialSparkIndex(entities: Array[Entity], radius: Double) {

  val rTree: STRtree = new STRtree()

  entities.foreach(x => {
    val y = x.env
    y.expandBy(radius)
    rTree.insert(y, x)
  })

  def query(env: Envelope): Array[AnyRef] = {
    rTree.query(env).toArray
  }

}
