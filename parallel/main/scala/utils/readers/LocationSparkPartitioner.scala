package utils.readers

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import model.TileGranularities
import model.entities.EntityType
import model.entities.geospark.{Entity, SpatialEntity}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import utils.configuration.Constants
import utils.geometryUtils.geospark.EnvelopeOp

import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}

case class LocationSparkPartitioner(source: SpatialRDD[Geometry],
                                    partitions: Int,
                                    gt: Constants.LocationSparkGridType.LocationSparkGridType = Constants.LocationSparkGridType.QUADTREE) {

  lazy val gridType: GridType = gt match {
    case Constants.LocationSparkGridType.EQUALGRID => GridType.EQUALGRID
    case _ => GridType.QUADTREE
  }

  val spatialPartitioner: SpatialPartitioner = {
    source.analyze()

//    gridType match {
//      case Constants.LocationSparkGridType.EQUALGRID =>

//        val paddedBoundary: Envelope = new Envelope(
//          source.boundaryEnvelope.getMinX(), source.boundaryEnvelope.getMaxX() + 0.01,
//          source.boundaryEnvelope.getMinY(), source.boundaryEnvelope.getMaxY() + 0.01);
//
//        val partitionsGrid: Int = ceil(sqrt(partitions)).toInt
//        val thetaX: Double = paddedBoundary.getWidth / partitionsGrid
//        val thetaY: Double = paddedBoundary.getHeight / partitionsGrid
//
//        var grids: List[Envelope] = List()
//        for (i <- 0 until partitionsGrid) {
//          for (j <- 0 until partitionsGrid) {
//            grids = grids :+ new Envelope(paddedBoundary.getMinX + thetaX * i, paddedBoundary.getMinX + thetaX * (i + 1),
//              paddedBoundary.getMinY + thetaY * j, paddedBoundary.getMinY + thetaY * (j + 1))
//          }
//        }
//        new FlatGridPartitioner(grids.asJava)

        if (partitions > 0)
          source.spatialPartitioning(gridType, partitions)
        else {
          source.spatialPartitioning(gridType)
        }
        source.getPartitioner

//      case _ =>
//        if (partitions > 0)
//          source.spatialPartitioning(GridType.QUADTREE, partitions)
//        else
//          source.spatialPartitioning(GridType.QUADTREE)
//        source.getPartitioner
//    }
  }

  lazy val approximateCount: Long = source.approximateTotalCount

  lazy val hashPartitioner: HashPartitioner = new HashPartitioner(spatialPartitioner.getGrids.size)

  lazy val partitionBorders: Seq[Envelope] = spatialPartitioner.getGrids.asScala


  def getPartitionsBorders(thetaOpt: Option[TileGranularities]): Array[Envelope] = {
    val partitionEnvelopes = thetaOpt match {
      case Some(theta) => partitionBorders.map(env => EnvelopeOp.adjust(env, theta))
      case None => partitionBorders
    }

    // get overall borders
    val globalMinX: Double = partitionEnvelopes.map(p => p.getMinX).min
    val globalMaxX: Double = partitionEnvelopes.map(p => p.getMaxX).max
    val globalMinY: Double = partitionEnvelopes.map(p => p.getMinY).min
    val globalMaxY: Double = partitionEnvelopes.map(p => p.getMaxY).max

    // make them integers - filtering is discrete
    val spaceMinX = math.floor(globalMinX).toInt - 1
    val spaceMaxX = math.ceil(globalMaxX).toInt + 1
    val spaceMinY = math.floor(globalMinY).toInt - 1
    val spaceMaxY = math.ceil(globalMaxY).toInt + 1

    partitionEnvelopes.map { env =>
      val minX = if (env.getMinX == globalMinX) spaceMinX else env.getMinX
      val maxX = if (env.getMaxX == globalMaxX) spaceMaxX else env.getMaxX
      val minY = if (env.getMinY == globalMinY) spaceMinY else env.getMinY
      val maxY = if (env.getMaxY == globalMaxY) spaceMaxY else env.getMaxY
      new Envelope(minX, maxX, minY, maxY)
    }.toArray
  }

  /**
   * Transform a Spatial RDD into an RDD of entities and spatial partition based
   * on the built spatial partitioner
   *
   * @param srdd       rdd to transform
   * @param entityType type of entity to transform to
   * @return a spatially distributed RDD of entities
   */
  def distributeAndTransform(srdd: SpatialRDD[Geometry], entityType: EntityType): RDD[(Int, Entity)] = {

    val partitionedRDD: RDD[(Int, Geometry)] = srdd.rawSpatialRDD.rdd
      .flatMap(geom => spatialPartitioner.placeObject(geom).asScala.map(i => (i._1.toInt, geom)))
      .partitionBy(hashPartitioner)

    val entitiesRDD: RDD[(Int, Entity)] = partitionedRDD.map {
      case (pid, geom) =>
        (pid, SpatialEntity(geom.getUserData.asInstanceOf[String], geom))
    }
    entitiesRDD
  }
}