package utils.readers

import index.partition.magellan.ZOrderCurveIndexer
import model.entities.{Entity, EntityType}
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Envelope, Geometry}

case class ZOrderCurvePartitioner(precision: Int, partitions: Int) {

  lazy val zOrderCurveIndexer: ZOrderCurveIndexer = new ZOrderCurveIndexer()

  val hashPartitioner: HashPartitioner = new HashPartitioner(partitions)

  var partitionBorders: Seq[Envelope] = Array[Envelope]()

  def distributeAndTransform(srdd: SpatialRDD[Geometry], entityType: EntityType): RDD[(Int, (Entity, Envelope))] = {

    val partitionedRDD: RDD[(Int, (Geometry, Envelope))] = srdd.rawSpatialRDD.rdd
      .flatMap(geom => {
        zOrderCurveIndexer
          .index(geom, precision)
          .map(curve => (Integer.parseInt(curve.code(), 2), (geom, curve.mbr)))
      })
      .partitionBy(hashPartitioner)

    val transformationF = entityType.transform
    val entitiesRDD: RDD[(Int, (Entity, Envelope))] = partitionedRDD.map{case (pid, (geom, curve)) => (pid, (transformationF(geom), curve))}
    entitiesRDD
  }

}
