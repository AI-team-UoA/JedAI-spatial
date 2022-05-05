package index.partition.spatialspark.fgp

import org.apache.spark.SparkContext
import org.locationtech.jts.geom.Envelope

class FixedGridPartition extends Serializable {

}

object FixedGridPartition {

  def apply(sc: SparkContext, extent: Envelope, gridDimX: Int, gridDimY: Int): Array[Envelope] = {

    val xSize = (extent.getMaxX - extent.getMinX) / gridDimX.toDouble
    val ySize = (extent.getMaxY - extent.getMinY) / gridDimY.toDouble
    val results = for (i <- Array.range(0, gridDimX); j <- Array.range(0, gridDimY))
      yield new Envelope(i * xSize + extent.getMinX, (i + 1) * xSize + extent.getMinX, j * ySize + extent.getMinY, (j + 1) * ySize + extent.getMinY)
    results
  }

}
