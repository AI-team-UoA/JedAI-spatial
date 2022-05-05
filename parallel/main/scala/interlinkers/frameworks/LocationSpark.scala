package interlinkers.frameworks

import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.SpatialIndex
import index.LocationSparkIndex
import index.partition.locationspark.SkewAnalysis
import index.partition.locationspark.SkewAnalysis.{repartitionSource, repartitionTarget}
import interlinkers.InterlinkerGeoSpark
import model.entities.geospark.Entity
import model.geospark.IM
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import utils.configuration.Constants.LocationSparkLocalIndex.LocationSparkLocalIndex
import utils.configuration.Constants.Relation.Relation

import scala.collection.immutable.TreeMap

case class  LocationSpark(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                          locationSparkLocalIndex: LocationSparkLocalIndex,
                          partitionBorders: Array[Envelope]) extends InterlinkerGeoSpark {

  def getAllCandidates(target: Entity, spatialIndex: SpatialIndex, partition: Envelope): Seq[Entity] = {
      spatialIndex.query(target.env).toArray
        .map(candidate => candidate.asInstanceOf[Entity])
        .filter(candidate => referencePointFiltering(target, candidate, partition))
  }

  override def relate(relation: Relation): RDD[(String, String)] = {
    joinedRDD
      .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
      .flatMap { p =>
        val pid = p._1
        val partition = partitionBorders(pid)
        val source: Array[Entity] = p._2._1.toArray
        val target: Iterator[Entity] = p._2._2.toIterator
        val sourceIndex = LocationSparkIndex(source, locationSparkLocalIndex)

        target.flatMap { e2 =>
          getAllCandidates(e2, sourceIndex, partition)
            .filter(e1 => e1.relate(e2, relation))
            .map(e1 => (e1.originalID, e2.originalID))
        }
      }
  }

  override def getDE9IM: RDD[IM] = {
    joinedRDD
      .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
      .flatMap { p =>
        val pid = p._1
        val partition = partitionBorders(pid)
        val source: Array[Entity] = p._2._1.toArray
        val target: Iterator[Entity] = p._2._2.toIterator
        val sourceIndex = LocationSparkIndex(source, locationSparkLocalIndex)

        target.flatMap { e2 =>
          getAllCandidates(e2, sourceIndex, partition)
            .map(s => s.getIntersectionMatrix(e2))
        }
      }
  }

}

object LocationSpark{

  def apply(source: RDD[(Int, Entity)],
            target: RDD[(Int, Entity)],
            indexType: LocationSparkLocalIndex,
            partitionBorders: Array[Envelope]): LocationSpark = {

    // Skew Analysis
    val stats = SkewAnalysis.analysis(source, target)
    val topKPartitions: TreeMap[Int, Int] = SkewAnalysis.findSkewPartitionQuery(stats, 0.5)

    // repartition of skewed partitions
    val newPartitioner = new HashPartitioner(partitionBorders.length + topKPartitions.size)
    val sourceRepartitioned = if (topKPartitions.isEmpty) source else repartitionSource(source, topKPartitions, newPartitioner)
    val targetRepartitioned = if (topKPartitions.isEmpty) target else repartitionTarget(target, topKPartitions, newPartitioner)
    val newPartitionBorders = partitionBorders ++ topKPartitions.map(x => partitionBorders(x._1))

    val joinedRDD = sourceRepartitioned.cogroup(targetRepartitioned, newPartitioner)
    LocationSpark(joinedRDD, indexType, newPartitionBorders)
  }

}
