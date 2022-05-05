package index

import model.entities.Entity
import org.locationtech.jts.index.SpatialIndex
import org.locationtech.jts.index.quadtree.Quadtree
import org.locationtech.jts.index.strtree.STRtree
import utils.configuration.Constants.SedonaLocalIndex.{NO_INDEX, QUADTREE, RTREE, SedonaLocalIndex}

object SedonaIndex {

  def apply(source: Array[Entity], localIndexType: SedonaLocalIndex): SpatialIndex = {

    localIndexType match {

      case RTREE =>
        val rtree = new STRtree()
        source.foreach(x => rtree.insert(x.env, x))
        rtree

      case QUADTREE =>
        val qtree = new Quadtree()
        source.foreach(x => qtree.insert(x.env, x))
        qtree

      case NO_INDEX =>
        null
    }
  }

}
