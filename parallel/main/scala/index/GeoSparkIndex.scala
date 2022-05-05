package index

import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import model.entities.geospark.Entity
import utils.configuration.Constants.GeoSparkLocalIndex.{GeoSparkLocalIndex, NO_INDEX, QUADTREE, RTREE}

object GeoSparkIndex {

  def apply(source: Array[Entity], localIndexType: GeoSparkLocalIndex): SpatialIndex = {

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
