package index

import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import index.partition.locationspark.EqualGrid
import model.entities.geospark.Entity
import utils.configuration.Constants.LocationSparkLocalIndex.{EQUALGRID, LocationSparkLocalIndex, QUADTREE, RTREE}

object LocationSparkIndex {

  def apply(source: Array[Entity],
            indexType: LocationSparkLocalIndex): SpatialIndex =

    indexType match {

      case RTREE =>
        val rtree = new STRtree()
        source.foreach(x => rtree.insert(x.env, x))
        rtree

      case QUADTREE =>
        val qtree = new Quadtree()
        source.foreach(x => qtree.insert(x.env, x))
        qtree

      case EQUALGRID =>

        val thetaX = source.map(mbr => mbr.env.getWidth).sum / source.length
        val thetaY = source.map(mbr => mbr.env.getHeight).sum / source.length

        val equalGrids = EqualGrid(thetaX, thetaY)
        source.foreach(x => equalGrids.insert(x.env, x))
        equalGrids
    }

}
