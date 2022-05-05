package index.partition.magellan

import org.locationtech.jts.geom.{Geometry, Point}
import utils.configuration.Constants.Relation.Relation

trait Indexer[T <: Index] extends Serializable {

  /**
   * Output the smallest spatial curve that covers given point at given precision
   *
   * @param point
   * @param precision
   * @return
   */
  def index(point: Point, precision: Int): T

  /**
   * Output the set of all spatial curves that cover the given shape at given precision
   *
   * @param shape
   * @param precision
   * @return
   */
  def index(shape: Geometry, precision: Int): Seq[T]

  /**
   * Outputs the set of all spatial curves that cover the given shape at a given precision,
   * along with metadata about the nature of the relation. A spatial index can either Contain
   * the shape, Intersect the shape, or be Within the shape.
   *
   * @param shape
   * @param precision
   * @return
   */
  def indexWithMeta(shape: Geometry, precision: Int): Seq[(T, Relation)]

}
