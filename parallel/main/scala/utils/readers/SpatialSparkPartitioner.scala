package utils.readers

import index.partition.spatialspark.PartitionConf
import model.entities.{Entity, EntityType}
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.locationtech.jts.index.strtree.STRtree
import index.partition.spatialspark.bsp.{BinarySplitPartition, BinarySplitPartitionConf}
import index.partition.spatialspark.fgp.{FixedGridPartition, FixedGridPartitionConf}
import index.partition.spatialspark.stp.{SortTilePartition, SortTilePartitionConf}
import utils.configuration.Constants
import utils.configuration.Constants.SpatialSparkPartitionMethod

case class SpatialSparkPartitioner(source: SpatialRDD[Geometry],
                                   partitions: Int,
                                   partitionTechnique: Constants.SpatialSparkPartitionMethod.SpatialSparkPartitionMethod,
                                   partitionConf: PartitionConf,
                                   radius: Double) {

  lazy val hashPartitioner: HashPartitioner = new HashPartitioner(partitions)

  var partitionBorders: Seq[Envelope] = Array[Envelope]()

  val rTree = new STRtree()

  partitionConf.partitionMethod match {
    case SpatialSparkPartitionMethod.STP =>
      partition(source, partitionConf.asInstanceOf[SortTilePartitionConf])

    case SpatialSparkPartitionMethod.BSP =>
      partition(source, partitionConf.asInstanceOf[BinarySplitPartitionConf])

    case SpatialSparkPartitionMethod.FGP =>
      partition(source.rawSpatialRDD.rdd.sparkContext, partitionConf.asInstanceOf[FixedGridPartitionConf])
  }


  def populateRTree(partitions: Array[(Envelope, Int)]): Unit = {
    for (i <- partitions) {
      val mbr = new Envelope(i._1.getMinX, i._1.getMaxX, i._1.getMinY, i._1.getMaxY)
      rTree.insert(mbr, i)
    }
  }


  def getPartitionsBorders: Array[Envelope] ={
    val partitionEnvelopes = partitionBorders

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


  def partition(source: SpatialRDD[Geometry], stpConf: SortTilePartitionConf): Unit = {

    val sampleData = source.rawSpatialRDD.rdd
      .sample(withReplacement = false, fraction = stpConf.ratio)
      .map(x => x.getEnvelopeInternal)

    val partitions = SortTilePartition(sampleData, stpConf.extent, stpConf.gridDimX, stpConf.gridDimY)
    partitionBorders = partitions
    val partitionsWithIndex = partitions.zipWithIndex
    populateRTree(partitionsWithIndex)
  }


  def partition(source: SpatialRDD[Geometry], bspConf: BinarySplitPartitionConf): Unit = {

    val sampleData = source.rawSpatialRDD.rdd
      .sample(withReplacement = false, fraction = bspConf.ratio)
      .map(x => x.getEnvelopeInternal)

    val partitions = BinarySplitPartition(sampleData, bspConf.extent, bspConf.level)
    partitionBorders = partitions
    val partitionsWithIndex = partitions.zipWithIndex
    populateRTree(partitionsWithIndex)
  }

  def partition(sc: SparkContext, fgConf: FixedGridPartitionConf): Unit = {

    val partitions = FixedGridPartition(sc, fgConf.extent, fgConf.gridDimX, fgConf.gridDimY)
    partitionBorders = partitions
    val partitionsWithIndex = partitions.zipWithIndex
    populateRTree(partitionsWithIndex)
  }


  def distributeAndTransform(srdd: SpatialRDD[Geometry], entityType: EntityType, radius: Double): RDD[(Int, Entity)] = {

    val rTreeBroadcast = srdd.rawSpatialRDD.rdd.sparkContext.broadcast(rTree)
    val rTreeLocal = rTreeBroadcast.value

    val partitionedRDD: RDD[(Int, Geometry)] = srdd.rawSpatialRDD.rdd
      .flatMap{ geom =>
        val queryEnv = geom.getEnvelopeInternal
        queryEnv.expandBy(radius)
        rTreeLocal.query(queryEnv).toArray.map { case (geom_, id_) => (id_.asInstanceOf[Int], geom) }
      }
      .partitionBy(hashPartitioner)

    val transformationF = entityType.transform
    val entitiesRDD: RDD[(Int, Entity)] = partitionedRDD.map{case (pid, geom) => (pid, transformationF(geom))}
    entitiesRDD
  }
}
