package index.partition.spatialspark.bsp

import index.partition.spatialspark.PartitionConf
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.SpatialSparkPartitionMethod


class BinarySplitPartitionConf(val ratio: Double, val extent: Envelope, val level: Long) extends PartitionConf(SpatialSparkPartitionMethod.BSP) with Serializable {

}
