/*
 * Copyright 2015 Simin You
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package index.partition.spatialspark.stp

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope

class SortTilePartition extends Serializable {

}

object SortTilePartition {

  case class Wrapped[A](elem: A)(implicit ordering: Ordering[A])
    extends Ordered[Wrapped[A]] {
    def compare(that: Wrapped[A]): Int = ordering.compare(this.elem, that.elem)
  }

  def apply(sampleData: RDD[Envelope], extent: Envelope, dimX: Int, dimY: Int): Array[Envelope] = {
    val numObjects = sampleData.count()
    val numObjectsPerTile = math.ceil(numObjects.toDouble / (dimX * dimY)).toLong
    val numObjectsPerSlice = numObjectsPerTile * dimY

    //sort by center_x, slice, center_y
    val centroids = sampleData.map(x => ((x.getMinX + x.getMaxX) / 2.0, (x.getMinY + x.getMaxY) / 2.0))
    val objs = centroids.sortByKey(ascending = true).zipWithIndex().map(x => (x._1._1, x._1._2, x._2))
    val objectsSorted = objs.map(x => (Wrapped(x._3 / numObjectsPerSlice, x._2), x))
      .sortByKey(ascending = true).values

    //pack
    val tiles = objectsSorted.zipWithIndex().map(x => (x._2 / numObjectsPerTile,
      (x._1._1, x._1._2, x._1._1, x._1._2, x._2 / numObjectsPerSlice, x._2 / numObjectsPerTile)))
      .reduceByKey((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4, a._5, a._6))
      .values
    //reduce for slice boundaries
    val sliceMap = tiles.map(x => (x._5, (x._1, x._3))).reduceByKey((a, b) => (a._1 min b._1, a._1 max b._1)).collectAsMap()
    //val sliceBoundsBroadcast = sc.broadcast(sliceBounds)
    //val tileMap = tiles.map()
    val tilesLocal = tiles.collect()
    val tileMap = tiles.keyBy(_._6).collectAsMap()
    //fill out tiles as continuous partitions
    val sliceBounds = sliceMap.map(x => x._1 ->(if (x._1 == 0) extent.getMinX else (x._2._1 + sliceMap(x._1 - 1)._2) / 2,
      if (x._1 == sliceMap.size - 1) extent.getMaxX else (x._2._2 + sliceMap(x._1 + 1)._1) / 2))
    val tilesFinal = tilesLocal.map(x => (sliceBounds(x._5)._1,
      if (x._6 == 0 || (x._6 != 0 && x._5 != tileMap(x._6 - 1)._5)) extent.getMinY else (x._2 + tileMap(x._6 - 1)._4) / 2,
      sliceBounds(x._5)._2,
      if (x._6 == tileMap.size - 1 || (x._6 != tileMap.size - 1 && x._5 != tileMap(x._6 + 1)._5)) extent.getMaxY else (x._4 + tileMap(x._6 + 1)._2) / 2
    ))

    tilesFinal.map(x => new Envelope(x._1, x._3, x._2, x._4))
  }

}
