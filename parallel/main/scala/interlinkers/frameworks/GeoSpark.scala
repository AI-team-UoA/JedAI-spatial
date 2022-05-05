package interlinkers.frameworks

import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.SpatialIndex
import index.GeoSparkIndex
import interlinkers.InterlinkerGeoSpark
import model.entities.geospark.Entity
import model.geospark.IM
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.configuration.Constants.GeoSparkLocalIndex.GeoSparkLocalIndex
import utils.configuration.Constants.Relation.Relation

case class GeoSpark(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                    geoSparkLocalIndex: GeoSparkLocalIndex,
                    partitionBorders:  Array[Envelope]) extends InterlinkerGeoSpark {

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
        val sourceIndex = GeoSparkIndex(source, geoSparkLocalIndex)

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
        val sourceIndex = GeoSparkIndex(source, geoSparkLocalIndex)

        target.flatMap { e2 =>
          getAllCandidates(e2, source, sourceIndex, partition)
            .map(s => s.getIntersectionMatrix(e2))
        }
      }

}

object GeoSpark{

  def apply(source: RDD[(Int, Entity)],
            target: RDD[(Int, Entity)],
            partitioner: Partitioner,
            localIndexType: GeoSparkLocalIndex,
            partitionBorders: Array[Envelope]): GeoSpark = {

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))] = source.cogroup(target, partitioner)
    GeoSpark(joinedRDD, localIndexType, partitionBorders)
  }

}