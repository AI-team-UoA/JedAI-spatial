/**
 * Copyright 2015 Ram Sriharsha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package index.partition.magellan

import model.PointObject
import org.locationtech.jts.geom.{Envelope, Geometry, Point}
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation

import scala.collection.mutable.ListBuffer

class ZOrderCurveIndexer(mbr: Envelope) extends Indexer[ZOrderCurve] {

  def this() = this(new Envelope(180, -180, 90, -90))

  override def index(point: Point, precision: Int): ZOrderCurve = {
    var currentPrecision = 0
    var evenBit = true
    var xrange = Array(mbr.getMinX, mbr.getMaxX)
    var yrange = Array(mbr.getMinY, mbr.getMaxY)
    var bits = 0L

    def encode(v: Double, range: Array[Double]): Unit = {
      val mid = range(0) + (range(1) - range(0))/ 2.0
      if (v < mid) {
        // add off bit
        bits <<= 1
        range.update(1, mid)
      } else {
        // add on bit
        bits <<= 1
        bits = bits | 0x1
        range.update(0, mid)
      }
      currentPrecision += 1
    }

    while (currentPrecision < precision) {
      if (evenBit) {
        encode(point.getX, xrange)
      } else {
        encode(point.getY, yrange)
      }
      evenBit = !evenBit
    }
    bits <<= (64 - precision)
    new ZOrderCurve(
      new Envelope(xrange(1), xrange(0), yrange(1), yrange(0)), precision, bits)
  }

  def relate(that: Envelope, other: Envelope): Relation = {
    if (that.contains(other)) {
      Relation.CONTAINS
    } else if (that.contains(other)) {//(lineIntersections == 0 && vertexContained == 4) {
      Relation.WITHIN
    } else if (that.intersects(other)) {//(lineIntersections > 0 || vertexContained > 0) {
      Relation.INTERSECTS
    } else {
      Relation.DISJOINT
    }
  }

  override def index(shape: Geometry, precision: Int): Seq[ZOrderCurve] = {
    indexWithMeta(shape, precision).map(_._1)
  }

  override def indexWithMeta(shape: Geometry, precision: Int): Seq[(ZOrderCurve, Relation)] = {
    shape match {
      case p: Point => ListBuffer((index(p, precision), Relation.CONTAINS))
      case _ =>
        val candidates = cover(shape.getEnvelopeInternal, precision)
        val results = new ListBuffer[(ZOrderCurve, Relation)]
        for (candidate <- candidates) {
          // check if the candidate actually lies within the shape
          val box = candidate.mbr
          val relation = relate(box, shape.getEnvelopeInternal)
          if (relation != Relation.DISJOINT) {
            results.+= ((candidate, relation))
          }
        }
        results
    }
  }

  /**
    * Returns the curves of a given precision that cover the bounding box.
    *
    * @param box
    * @return
    */
  def cover(box: Envelope, precision: Int): ListBuffer[ZOrderCurve] = {
    val leftBottom = index(PointObject(box.getMinX, box.getMinY), precision)
    val xdelta = Math.abs(leftBottom.mbr.getMaxX - leftBottom.mbr.getMinX)
    val ydelta = Math.abs(leftBottom.mbr.getMaxY - leftBottom.mbr.getMinY)

    val cover = new ListBuffer[ZOrderCurve]()

    var i = leftBottom.mbr.getMinX

    while (i <= box.getMaxX) {
      var j = leftBottom.mbr.getMinY
      while (j <= box.getMaxY) {
        val candidate = index(PointObject(i, j), precision)
        // check if the candidate intersects the box
        if (box.intersects(candidate.mbr)) {
          cover.+= (index(PointObject(i, j), precision))
        }
        j += ydelta
      }

      i += xdelta
    }

    cover
  }

}



