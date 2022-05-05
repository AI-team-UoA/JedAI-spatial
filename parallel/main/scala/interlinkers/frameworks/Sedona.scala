package interlinkers.frameworks

import index.SedonaIndex
import interlinkers.InterlinkerFramework
import model.IM
import model.entities.Entity
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.index.SpatialIndex
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.SedonaLocalIndex.SedonaLocalIndex


case class Sedona(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                  sedonaLocalIndex: SedonaLocalIndex,
                  partitionBorders:  Array[Envelope]) extends InterlinkerFramework {

  def getAllCandidates(target: Entity, source: Array[Entity], spatialIndex: SpatialIndex, partition: Envelope): Seq[Entity] =
    if (spatialIndex == null) getAllCandidates(target, source, partition) else getAllCandidates(target, spatialIndex, partition)

  def getAllCandidates(target: Entity, spatialIndex: SpatialIndex, partition: Envelope): Seq[Entity] = spatialIndex
    .query(target.env).toArray
    .map(candidate => candidate.asInstanceOf[Entity])
    .filter(candidate => referencePointFiltering(target, candidate, partition))

  def getAllCandidates(target: Entity, source: Array[Entity], partition: Envelope): Seq[Entity] = source
    .map(candidate => candidate)
    .filter(candidate => referencePointFiltering(target, candidate, partition))

  override def relate(relation: Relation): RDD[(String, String)] = joinedRDD
    .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
    .flatMap { p =>
      val pid = p._1
      val partition = partitionBorders(pid)
      val source: Array[Entity] = p._2._1.toArray
      val target: Iterator[Entity] = p._2._2.toIterator
      val sourceIndex = SedonaIndex(source, sedonaLocalIndex)

      target.flatMap { e2 =>
        getAllCandidates(e2, source, sourceIndex, partition)
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
      val sourceIndex = SedonaIndex(source, sedonaLocalIndex)

      target.flatMap { e2 =>
        getAllCandidates(e2, source, sourceIndex, partition)
          .map(s => s.getIntersectionMatrix(e2))
      }
    }

}

object Sedona {

  def apply(source: RDD[(Int, Entity)],
            target: RDD[(Int, Entity)],
            partitioner: Partitioner,
            localIndexType: SedonaLocalIndex,
            partitionBorders: Array[Envelope]): Sedona = {

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))] = source.cogroup(target, partitioner)
    Sedona(joinedRDD, localIndexType, partitionBorders)
  }
}

