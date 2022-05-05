package interlinkers.frameworks

import index.partition.spatialspark.PartitionConf
import interlinkers.InterlinkerFramework
import model.IM
import model.entities.Entity
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.Relation.Relation

case class SpatialSparkPartitioned(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                                   partitionBorders: Array[Envelope]) extends InterlinkerFramework {

  override def relate(relation: Relation): RDD[(String, String)] = joinedRDD
    .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
    .flatMap { p =>
      val pid = p._1
      val partition = partitionBorders(pid)
      val source: Array[Entity] = p._2._1.toArray
      val target: Iterator[Entity] = p._2._2.toIterator

      target.flatMap{ e2 =>
        source
          .filter(e1 => referencePointFiltering(e2, e1, partition))
          .filter(e1 => e1.relate(e2, relation))
          .map(e1 => (e1.originalID, e2.originalID))
      }
    }

  override def getDE9IM: RDD[IM] = joinedRDD
    .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
    .flatMap { p =>
      val pid = p._1
      val partition = partitionBorders(pid)
      val source: Array[Entity] = p._2._1.toArray
      val target: Iterator[Entity] = p._2._2.toIterator

      target.flatMap{ e2 =>
        source
          .filter(e1 => referencePointFiltering(e2, e1, partition))
          .map(s => s.getIntersectionMatrix(e2))
      }
    }

}


object SpatialSparkPartitioned{

  def apply(source:RDD[(Int, Entity)],
            target:RDD[(Int, Entity)],
            partitionConf: PartitionConf,
            partitioner: Partitioner,
            partitionBorders: Array[Envelope],
            radius: Double): SpatialSparkPartitioned = {

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))] = source.cogroup(target, partitioner)
    SpatialSparkPartitioned(joinedRDD, partitionBorders)
  }
}

