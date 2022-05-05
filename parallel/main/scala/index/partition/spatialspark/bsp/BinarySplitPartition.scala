package index.partition.spatialspark.bsp

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope

class BinarySplitPartition extends Serializable {

}

object BinarySplitPartition {

  def apply(sampleData: RDD[Envelope], extent: Envelope, level: Long): Array[Envelope] = {
    val centroids = sampleData.map(x => ((x.getMinX + x.getMaxX) / 2.0, (x.getMinY + x.getMaxY) / 2.0))
    val partitions = binaryPartition(centroids, (extent.getMinX, extent.getMaxX, extent.getMinY, extent.getMaxY), level)
    partitions.map(x => new Envelope(x._1, x._2, x._3, x._4)).collect()
  }

  def binaryPartition(points: RDD[(Double, Double)],
                      extent: (Double, Double, Double, Double), level: Long): RDD[(Double, Double, Double, Double)] = {

    val numPoints = points.count()
    val halfNumPoints = numPoints / 2
    if (numPoints < 3 || level == 0)
      return points.sparkContext.parallelize(List(extent))

    var result = points.sparkContext.parallelize(List.empty[(Double, Double, Double, Double)])

    if (extent._2 - extent._1 > extent._4 - extent._3) {
      val pointsSorted = points.sortByKey(ascending = true).cache()
      val first = pointsSorted.zipWithIndex().filter(_._2 / halfNumPoints == 0).map(_._1)
      val second = pointsSorted.subtract(first)
      val bound = first.map(_._1).max()
      val firstExtent = (extent._1, bound, extent._3, extent._4)
      val secondExtent = (bound, extent._2, extent._3, extent._4)
      result ++= binaryPartition(first, firstExtent, level - 1)
      result ++= binaryPartition(second, secondExtent, level - 1)
    }
    else {
      val pointsSorted = points.keyBy(_._2).sortByKey(ascending = true).values.cache()
      val first = pointsSorted.zipWithIndex().filter(_._2 / halfNumPoints == 0).map(_._1)
      val second = pointsSorted.subtract(first)
      val bound = first.map(_._2).max()
      val firstExtent = (extent._1, extent._2, extent._3, bound)
      val secondExtent = (extent._1, extent._2, bound, extent._4)
      result ++= binaryPartition(first, firstExtent, level - 1)
      result ++= binaryPartition(second, secondExtent, level - 1)
    }
    result.cache()
  }

}