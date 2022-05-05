package interlinkers.frameworks

import interlinkers.InterlinkerFramework
import model.IM
import model.entities.Entity
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.Relation.Relation

case class Magellan(joinedRDD: RDD[(Int, (Iterable[(Entity, Envelope)], Iterable[(Entity, Envelope)]))]) extends InterlinkerFramework {

  override def relate(relation: Relation): RDD[(String, String)] = joinedRDD
    .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
    .flatMap{ p =>
      val source: Array[(Entity, Envelope)] = p._2._1.toArray
      val target: Iterator[(Entity, Envelope)] = p._2._2.toIterator

      target.flatMap{ e2 =>
        source
          .filter(e1 => referencePointFiltering(e2._1, e1._1, e2._2))
          .filter(e1 => e1._1.relate(e2._1, relation))
          .map(e1 => (e1._1.originalID, e2._1.originalID))
      }
    }

  override def getDE9IM: RDD[IM] = joinedRDD
    .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
    .flatMap{ p =>
      val source: Array[(Entity, Envelope)] = p._2._1.toArray
      val target: Iterator[(Entity, Envelope)] = p._2._2.toIterator

      target.flatMap{ e2 =>
        source
          .filter(e1 => referencePointFiltering(e2._1, e1._1, e2._2))
          .map(s => s._1.getIntersectionMatrix(e2._1))
      }
    }

}

object Magellan{

  def apply(source: RDD[(Int, (Entity, Envelope))],
            target: RDD[(Int, (Entity, Envelope))],
            precision: Int,
            partitioner: Partitioner): Magellan = {

    val joinedRDD: RDD[(Int, (Iterable[(Entity, Envelope)], Iterable[(Entity, Envelope)]))] = source.cogroup(target, partitioner)
    Magellan(joinedRDD)
  }
}
