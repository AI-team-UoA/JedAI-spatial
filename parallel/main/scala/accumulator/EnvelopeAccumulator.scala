package accumulator

import org.apache.spark.util.AccumulatorV2
import org.locationtech.jts.geom.Envelope

object EnvelopeAccumulator extends AccumulatorV2[Envelope, Envelope] {

  private val area: Envelope = new Envelope()

  override def isZero: Boolean = area.getMinX == 0 && area.getMaxX == -1 && area.getMinY == 0 && area.getMaxY == -1

  override def copy(): AccumulatorV2[Envelope, Envelope] = EnvelopeAccumulator

  override def reset(): Unit = area.init()

  override def add(v: Envelope): Unit = area.expandToInclude(v)

  override def merge(other: AccumulatorV2[Envelope, Envelope]): Unit = area.expandToInclude(other.value)

  override def value: Envelope = area

}
