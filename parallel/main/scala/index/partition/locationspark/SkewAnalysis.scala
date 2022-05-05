package index.partition.locationspark

import model.entities.geospark.Entity
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeMap
import scala.collection.mutable

object SkewAnalysis {

  def repartitionSource(source: RDD[(Int, Entity)],
                        topKPartitions: TreeMap[Int, Int],
                        partitioner: Partitioner): RDD[(Int, Entity)] = {

    val repartitionHash: Map[Int, Int] = topKPartitions.zipWithIndex.map(x => (x._1._1, x._2 + source.partitions.length))
    val repartitionHashBroadcast: Broadcast[Map[Int, Int]] = source.context.broadcast(repartitionHash)
    val topKPartitionsBroadcast: Broadcast[TreeMap[Int, Int]] = source.context.broadcast(topKPartitions)

    // assign an id for each entity in the partition and the size of the partition
    val sourceWithIndexedPartitions: RDD[(((Int, Entity), Int), Int)] = source
      .mapPartitions(iter => {
        val newIter = iter.toArray
        newIter.zipWithIndex.map(x => x -> newIter.length).toIterator
      })

    // split the entities which have an id greater than the size of the partition/2
    val sourceSplit = sourceWithIndexedPartitions
      .filter(x => topKPartitionsBroadcast.value.contains(x._1._1._1) && x._1._2 > x._2 / 2)
      .map(x => x._1._1)
      .map(x => repartitionHashBroadcast.value(x._1) -> x._2)

    val newSource = sourceWithIndexedPartitions
      .filter(x => (topKPartitionsBroadcast.value.contains(x._1._1._1) && x._1._2 <= x._2 / 2) || !topKPartitionsBroadcast.value.contains(x._1._1._1))
      .map(x => x._1._1)

    newSource.union(sourceSplit).partitionBy(partitioner)
  }

  def repartitionTarget(target: RDD[(Int, Entity)],
                        topKPartitions: TreeMap[Int, Int],
                        partitioner: Partitioner): RDD[(Int, Entity)] = {

    val repartitionHash: Map[Int, Int] = topKPartitions.zipWithIndex.map(x => (x._1._1, x._2 + target.partitions.length))

    val repartitionHashBroadcast: Broadcast[Map[Int, Int]] = target.context.broadcast(repartitionHash)
    val topKPartitionsBroadcast: Broadcast[TreeMap[Int, Int]] = target.context.broadcast(topKPartitions)

    val targetDup: RDD[(Int, Entity)] = target
      .filter(x => topKPartitionsBroadcast.value.contains(x._1))
      .map(x => repartitionHashBroadcast.value(x._1) -> x._2)

    target.union(targetDup).partitionBy(partitioner)
  }

  private def getPartitionSize[T](rdd: RDD[T]): (Array[(Int, Int)]) = {
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      Iterator((idx, iter.size))
    }.collect()
    sketched
  }

  def analysis(data: RDD[(Int, Entity)], query: RDD[(Int, Entity)]): IndexedSeq[(Int, Int, Int)] = {

    val stat_datardd = getPartitionSize(data).sortBy(_._1)
    val stat_queryrdd = getPartitionSize(query).sortBy(_._1)

    stat_datardd.map {
      case (id, size) =>
        stat_queryrdd.find(_._1 == id).getOrElse(None)
        match {
          case (qid: Int, qsize: Int) => (id, size, qsize)
          case None => (id, size, 0)
        }
    }.toIndexedSeq
  }

  def findSkewPartitionQuery(stat: IndexedSeq[(Int, Int, Int)], threshold: Double): TreeMap[Int, Int] = {

    val map = mutable.HashMap.empty[Int,Int]

    //find those skew partitions,
    val sortlist = stat.sortBy(r => (r._3))(Ordering[Int].reverse)
    val templist = sortlist.filter(x => x._3 > 0)
    val topk = (templist.size * threshold).toInt
    val base = templist(topk)._3

    sortlist.foreach {
      element =>
        if (element._3 / base >= 3) {
          map.+= (element._1->(element._3 / base))
        }
    }

    TreeMap(map.toSeq:_*)
  }

}
