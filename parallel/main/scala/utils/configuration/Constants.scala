package utils.configuration

object Constants {

	final val defaultDatePattern = "yyyy-MM-dd HH:mm:ss"


	/**
	 * Relations
	 */
	object Relation extends Enumeration {
		type Relation = Value
		val EQUALS: Relation.Value = Value("equals")
		val DISJOINT: Relation.Value = Value("disjoint")
		val INTERSECTS: Relation.Value = Value("intersects")
		val TOUCHES: Relation.Value = Value("touches")
		val CROSSES: Relation.Value = Value("crosses")
		val WITHIN: Relation.Value = Value("within")
		val CONTAINS: Relation.Value = Value("contains")
		val OVERLAPS: Relation.Value = Value("overlaps")
		val COVERS: Relation.Value = Value("covers")
		val COVEREDBY: Relation.Value = Value("coveredby")
		val DE9IM: Relation.Value = Value("DE9IM")

		def exists(s: String): Boolean = values.exists(_.toString == s)

		def swap(r: Relation): Relation = r match {
			case Relation.WITHIN => Relation.CONTAINS
			case Relation.CONTAINS => Relation.WITHIN
			case Relation.COVERS => Relation.COVEREDBY
			case Relation.COVEREDBY => Relation.COVERS;
			case _ => r
		}
	}

	object ThetaOption extends Enumeration{
		type ThetaOption = Value
		val MAX: Constants.ThetaOption.Value = Value("max")
		val MIN: Constants.ThetaOption.Value = Value("min")
		val AVG: Constants.ThetaOption.Value = Value("avg")
		val AVG_x2: Constants.ThetaOption.Value = Value("avg2")
		val NO_USE: Constants.ThetaOption.Value = Value("none")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	/**
	 * Supported fileTypes
	 */
	object FileTypes extends Enumeration{
		type FileTypes = Value
		val NTRIPLES: Constants.FileTypes.Value = Value("nt")
		val TURTLE: Constants.FileTypes.Value = Value("ttl")
		val RDFXML: Constants.FileTypes.Value = Value("xml")
		val RDFJSON: Constants.FileTypes.Value = Value("rj")
		val CSV: Constants.FileTypes.Value = Value("csv")
		val TSV: Constants.FileTypes.Value = Value("tsv")
		val SHP: Constants.FileTypes.Value = Value("shp")
		val GEOJSON: Constants.FileTypes.Value = Value("geojson")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	/**
	 * Weighting Strategies
	 */
	object WeightingFunction extends Enumeration {
		type WeightingFunction = Value

		// co-occurrence frequency
		val CF: Constants.WeightingFunction.Value = Value("CF")

		// jaccard  similarity
		val JS: Constants.WeightingFunction.Value = Value("JS")

		// Pearson's chi squared test
		val PEARSON_X2: Constants.WeightingFunction.Value = Value("PEARSON_X2")

		// minimum bounding rectangle overlap
		val MBRO: Constants.WeightingFunction.Value = Value("MBRO")

		// inverse sum of points
		val ISP: Constants.WeightingFunction.Value = Value("ISP")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	/**
	 * YAML/command line Configurations arguments
	 */
	object InputConfigurations extends Enumeration {
		type YamlConfiguration = String
		val CONF_CONFIGURATIONS = "configurations"
		val CONF_PARTITIONS = "partitions"
		val CONF_THETA_GRANULARITY = "thetaGranularity"
		val CONF_PROGRESSIVE_ALG = "progressiveAlgorithm"
		val CONF_MAIN_WF = "mainWF"
		val CONF_SECONDARY_WF = "secondaryWF"
		val CONF_BUDGET = "budget"
		val CONF_GRID_TYPE = "gridType"
		val CONF_WS = "ws"
		val CONF_OUTPUT = "outputPath"
		val CONF_ENTITY_TYPE = "entityType"
		val CONF_STATISTICS = "stats"
		val CONF_TOTAL_VERIFICATIONS = "totalVerifications"
		val CONF_QUALIFYING_PAIRS = "qualifyingPairs"
		val CONF_DECOMPOSITION_THRESHOLD = "decompositionThreshold"
		val CONF_UNRECOGNIZED = "unrecognized"

		// Spatial Spark
		val CONF_SPATIAL_SPARK_EXTENT = "spatialSparkExtent"
		val CONF_SPATIAL_SPARK_METHOD = "spatialSparkMethod"
		val CONF_SPATIAL_SPARK_METHOD_CONF = "spatialSparkMethodConf"
		val CONF_SPATIAL_SPARK_RADIUS = "spatialSparkRadius"

		// Location Spark
		val CONF_LOCATION_SPARK_GRID_TYPE = "locationSparkGridType"
		val CONF_LOCATION_SPARK_LOCAL_INDEX_TYPE = "locationSparkLocalIndexType"
		val CONF_LOCATION_SPARK_GRID_PARTITIONS = "locationSparkGridPartitions"

		// Magellan
		val CONF_MAGELLAN_Z_ORDER_CURVE_PRECISION = "magellanZOrderCurvePrecision"

		// GeoSpark
		val CONF_GEO_SPARK_LOCAL_INDEX_TYPE = "geoSparkLocalIndexType"
		val CONF_GEO_SPARK_GRID_TYPE = "geoSparkGridType"

		// Sedona
		val CONF_SEDONA_LOCAL_INDEX_TYPE = "sedonaLocalIndexType"
		val CONF_SEDONA_GRID_TYPE = "sedonaGridType"
	}

	object GridType extends Enumeration{
		type GridType = Value
		val RTREE: Constants.GridType.Value = Value("RTREE")
		val KDBTREE: Constants.GridType.Value = Value("KDBTREE")
		val QUADTREE: Constants.GridType.Value = Value("QUADTREE")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	object SedonaGridType extends Enumeration {
		type SedonaGridType = Value
		val KDBTREE: Constants.SedonaGridType.Value = Value("KDBTREE")
		val QUADTREE: Constants.SedonaGridType.Value = Value("QUADTREE")
	}

	object GeoSparkGridType extends Enumeration {
		type GeoSparkGridType = Value
		val RTREE: Constants.GeoSparkGridType.Value = Value("RTREE")
		val KDBTREE: Constants.GeoSparkGridType.Value = Value("KDBTREE")
		val QUADTREE: Constants.GeoSparkGridType.Value = Value("QUADTREE")
		val EQUALGRID: Constants.GeoSparkGridType.Value = Value("EQUALGRID")
		val HILBERT: Constants.GeoSparkGridType.Value = Value("HILBERT")
		val VORONOI: Constants.GeoSparkGridType.Value = Value("VORONOI")
	}

	object LocationSparkGridType extends Enumeration {
		type LocationSparkGridType = Value
		val EQUALGRID: Constants.LocationSparkGridType.Value = Value("EQUALGRID")
		val QUADTREE: Constants.LocationSparkGridType.Value = Value("QUADTREE")
	}

	object SpatialSparkPartitionMethod extends Enumeration {
		type SpatialSparkPartitionMethod = Value
		val FGP: Constants.SpatialSparkPartitionMethod.Value = Value("FGP")
		val BSP: Constants.SpatialSparkPartitionMethod.Value = Value("BSP")
		val STP: Constants.SpatialSparkPartitionMethod.Value = Value("STP")
	}

	object LocationSparkLocalIndex extends Enumeration {
		type LocationSparkLocalIndex = Value
		val RTREE: Constants.LocationSparkLocalIndex.Value = Value("RTREE")
		val QUADTREE: Constants.LocationSparkLocalIndex.Value  = Value("QUADTREE")
		val EQUALGRID: Constants.LocationSparkLocalIndex.Value  = Value("EQUALGRID")
	}

	object GeoSparkLocalIndex extends Enumeration {
		type GeoSparkLocalIndex = Value
		val RTREE: Constants.GeoSparkLocalIndex.Value = Value("RTREE")
		val QUADTREE: Constants.GeoSparkLocalIndex.Value = Value("QUADTREE")
		val NO_INDEX: Constants.GeoSparkLocalIndex.Value = Value("NO_INDEX")
	}

	object SedonaLocalIndex extends Enumeration {
		type SedonaLocalIndex = Value
		val RTREE: Constants.SedonaLocalIndex.Value = Value("RTREE")
		val QUADTREE: Constants.SedonaLocalIndex.Value = Value("QUADTREE")
		val NO_INDEX: Constants.SedonaLocalIndex.Value = Value("NO_INDEX")
	}

	object EntityTypeENUM extends Enumeration {
		type EntityTypeENUM = Value
		val SPATIAL_ENTITY: Constants.EntityTypeENUM.Value = Value("SPATIAL_ENTITY")
		val SPATIOTEMPORAL_ENTITY: Constants.EntityTypeENUM.Value = Value("SPATIOTEMPORAL_ENTITY")
		val FRAGMENTED_ENTITY: Constants.EntityTypeENUM.Value = Value("FRAGMENTED_ENTITY")
		val INDEXED_FRAGMENTED_ENTITY: Constants.EntityTypeENUM.Value = Value("INDEXED_FRAGMENTED_ENTITY")
		val FINEGRAINED_ENTITY: Constants.EntityTypeENUM.Value = Value("FINEGRAINED_ENTITY")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	/**
	 * Progressive Algorithms
	 */
	object ProgressiveAlgorithm extends Enumeration {
		type ProgressiveAlgorithm = Value
		val PROGRESSIVE_GIANT: Constants.ProgressiveAlgorithm.Value = Value("PROGRESSIVE_GIANT")
		val DYNAMIC_PROGRESSIVE_GIANT: Constants.ProgressiveAlgorithm.Value = Value("DYNAMIC_PROGRESSIVE_GIANT")
		val GEOMETRY_CENTRIC: Constants.ProgressiveAlgorithm.Value = Value("GEOMETRY_CENTRIC")
		val TOPK: Constants.ProgressiveAlgorithm.Value = Value("TOPK")
		val RECIPROCAL_TOPK: Constants.ProgressiveAlgorithm.Value = Value("RECIPROCAL_TOPK")
		val RANDOM: Constants.ProgressiveAlgorithm.Value = Value("RANDOM")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	sealed trait WeightingScheme extends Serializable {val value: String }
	case object SIMPLE extends WeightingScheme {val value = "SIMPLE"}
	case object COMPOSITE extends WeightingScheme {val value = "COMPOSITE"}
	case object HYBRID extends WeightingScheme {val value = "HYBRID"}

	def WeightingSchemeFactory(ws: String): WeightingScheme ={
		ws.toLowerCase() match {
			case "simple" => SIMPLE
			case "composite" => COMPOSITE
			case "hybrid" => HYBRID
			case _ => SIMPLE
		}
	}

	def checkWS(ws: String): Boolean ={
		ws.toLowerCase() match {
			case "single" | "composite" | "hybrid" => true
			case _ => false
		}
	}

}
