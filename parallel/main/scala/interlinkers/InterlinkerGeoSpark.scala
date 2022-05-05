package interlinkers

import com.vividsolutions.jts.geom.Envelope
import model.entities.geospark.Entity
import model.geospark.IM
import org.apache.spark.rdd.RDD
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.geospark.EnvelopeOp

trait InterlinkerGeoSpark {

    def referencePointFiltering(s: Entity, t: Entity, partition: Envelope): Boolean = {
      val env1 = s.env
      val env2 = t.env
      val (rfX, rfY) = EnvelopeOp.getReferencePoint(env1, env2)

      val partitionContainsRF: Boolean = partition.getMinX <= rfX && partition.getMaxX >= rfX && partition.getMinY <= rfY && partition.getMaxY >= rfY
      partitionContainsRF
    }

    def relate(relation: Relation): RDD[(String, String)]

    def getDE9IM: RDD[IM]

    def countVerification: Long = getDE9IM.count()

    def countRelation(relation: Relation): Long = relate(relation).count()

  }

