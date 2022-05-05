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

import index.partition.spatialspark.PartitionConf
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.SpatialSparkPartitionMethod

class SortTilePartitionConf(val gridDimX:Int, val gridDimY:Int, val extent: Envelope, val ratio:Double) extends PartitionConf(SpatialSparkPartitionMethod.STP) with Serializable {

}
