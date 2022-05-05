package index.partition.locationspark

import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.{ItemVisitor, SpatialIndex}

import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.{ceil, floor, sqrt}

case class EqualGrid(grids: mutable.HashMap[Int, mutable.HashMap[Int, ArrayBuffer[Any]]], thetaX: Double, thetaY: Double) extends SpatialIndex{

  override def insert(envelope: Envelope, o: Any): Unit = {
    for (latIndex <- floor(envelope.getMinX / thetaX).toInt to ceil(envelope.getMaxX / thetaX).toInt) {
      for (longIndex <- floor(envelope.getMinY / thetaY).toInt to ceil(envelope.getMaxY / thetaY).toInt) {

        val latList = grids.get(latIndex)
        latList match {
          case Some(_) =>
            val longList = latList.get.get(longIndex)
            longList match {
              case Some(_) => grids(latIndex)(longIndex) += o
              case None => latList.get.put(longIndex, ArrayBuffer[Any](o))
            }
          case None =>
            grids.put(latIndex, mutable.HashMap[Int, ArrayBuffer[Any]]((longIndex, ArrayBuffer[Any](o))))
        }
      }
    }
  }

  override def query(envelope: Envelope): util.List[_] = {

    val candidates = new util.HashSet[Any]

    for (latIndex <- floor(envelope.getMinX / thetaX).toInt to ceil(envelope.getMaxX / thetaX).toInt) {
      for (longIndex <- floor(envelope.getMinY / thetaY).toInt to ceil(envelope.getMaxY / thetaY).toInt) {

        val values = for {
          x <- grids.get(latIndex)
          y <- x.get(longIndex)
        } yield y

        for (i <- values.getOrElse(ArrayBuffer()).indices) {
          candidates.add(values.get(i))
        }

      }
    }

    val candidateList = new util.ArrayList[Any](candidates)
    candidateList
  }

  override def query(envelope: Envelope, itemVisitor: ItemVisitor): Unit = ???

  override def remove(envelope: Envelope, o: Any): Boolean = ???
}

object EqualGrid{

  def apply(area: Envelope, partitions: Int): EqualGrid ={

    // find factor pairs with least sum for partitions
    var partitionsX: Int = 0
    var partitionsY: Int = 0
    var sum = Int.MaxValue
    for (i <- 1 to ceil(sqrt(partitions)).toInt) {
      if (partitions % i == 0 && partitions/i + i < sum){
        partitionsX = i
        partitionsY = partitions/i
        sum = partitionsX + partitionsY
      }
    }

    val thetaX = (area.getMaxX - area.getMinX) / partitionsX
    val thetaY = (area.getMaxY - area.getMinY) / partitionsY
    val grids = mutable.HashMap[Int, mutable.HashMap[Int, ArrayBuffer[Any]]]()

    new EqualGrid(grids, thetaX, thetaY)
  }

  def apply(thetaX: Double, thetaY: Double): EqualGrid ={
    val grids = mutable.HashMap[Int, mutable.HashMap[Int, ArrayBuffer[Any]]]()
    new EqualGrid(grids, thetaX, thetaY)
  }
}