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

import org.locationtech.jts.geom.Envelope

class ZOrderCurve(
    override val mbr: Envelope,
    override val precision: Int,
    override val bits: Long) extends Index {

  def children(): Seq[ZOrderCurve] = {
    val (xmid, ymid) = (mbr.getMinX + (mbr.getMaxX - mbr.getMinX)/ 2.0, mbr.getMinY + (mbr.getMaxY - mbr.getMinY)/ 2.0)
    val b:Long = bits >> (64 - precision)
    val parts = Array(
      (new Envelope(xmid, mbr.getMinX, mbr.getMinY, mbr.getMinY), {b << 2 | 0x0}),
      (new Envelope(xmid, mbr.getMinX, mbr.getMaxY, ymid), {b << 2 | 0x1}),
      (new Envelope(mbr.getMaxX, xmid, ymid, mbr.getMinY), {(b << 1 | 0x1) << 1}),
      (new Envelope(mbr.getMaxX, xmid, mbr.getMaxY, ymid), {((b << 1 | 0x1) << 1) | 0x1})
    )

    parts.map(v => new ZOrderCurve(v._1, precision + 2, v._2 << (62 - precision)))
  }

  def code(): String = {
    val sb = new StringBuilder
    var b = this.bits
    var i = 0
    val FLAG = 0x8000000000000000l
    while (i < precision) {
      if ((b & FLAG) == FLAG) {
        sb.append('1')
      } else {
        sb.append('0')
      }
      b <<= 1
      i += 1
    }
    sb.toString()
  }

  override def toBase32: String = {
    val BASE32 = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
      'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    if (precision % 5 != 0) {
      throw new IllegalStateException("Cannot convert a geohash to base32 if the precision is not a multiple of 5.")
    }
    val sb = new StringBuilder()

    val firstFiveBitsMask = 0xf800000000000000l
    var b = bits
    val partialChunks = Math.ceil((precision / 5)).toInt

    var i = 0
    while (i < partialChunks) {
      val pointer = ((b & firstFiveBitsMask) >>> 59).toInt
      sb.append(BASE32(pointer))
      b <<= 5
      i += 1
    }
    sb.toString()
  }

  override def toString = s"ZOrderCurve(${mbr.getMinX}, ${mbr.getMinY}, ${mbr.getMaxX}, ${mbr.getMaxY}, $precision, $bits, $code)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[ZOrderCurve]

  override def equals(other: Any): Boolean = other match {
    case that: ZOrderCurve =>
      (that canEqual this) &&
        precision == that.precision &&
        bits == that.bits
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(precision, bits)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
