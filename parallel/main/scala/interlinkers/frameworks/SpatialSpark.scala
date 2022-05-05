package interlinkers.frameworks

import index.SpatialSparkIndex
import interlinkers.InterlinkerFramework
import model.IM
import model.entities.Entity
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.configuration.Constants.Relation.Relation

case class SpatialSpark(sourceRDD: RDD[Entity],
                        index: Broadcast[SpatialSparkIndex]) extends InterlinkerFramework {

  def getAllCandidates(entity: Entity, index: SpatialSparkIndex): Seq[Entity] = {
    index.query(entity.env)
      .map(candidate => candidate.asInstanceOf[Entity])
  }

   override def relate(relation: Relation): RDD[(String, String)] = {
     val strTree = index.value
     sourceRDD.flatMap { entity =>
       getAllCandidates(entity, strTree)
         .filter(candidate => candidate.relate(entity, relation))
         .map(verified => (verified.originalID, entity.originalID))
     }
   }

  override def getDE9IM: RDD[IM] = {
    val strTree = index.value
    sourceRDD.flatMap { entity =>
      getAllCandidates(entity, strTree)
        .map(candidate => candidate.getIntersectionMatrix(entity))
    }
  }

}

object SpatialSpark{

  def apply(source:RDD[Entity], target:RDD[Entity], radius: Double): SpatialSpark ={

    val index = _root_.index.SpatialSparkIndex(target.collect, radius)
    val strTree = target.context.broadcast(index)
    SpatialSpark(source, strTree)
  }
}
