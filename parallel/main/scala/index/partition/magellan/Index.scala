package index.partition.magellan

import org.locationtech.jts.geom.Envelope

/**
 * An abstraction for a spatial curve based 2D Index.
 * A spatial curve represents a two dimensional grid of a given precision.
 */
trait Index extends Serializable {

  def precision(): Int

  def code(): String

  def bits(): Long

  def mbr(): Envelope

  def toBase32: String

}
