package utils


import cats.implicits._
import index.partition.spatialspark.PartitionConf
import index.partition.spatialspark.bsp.BinarySplitPartitionConf
import index.partition.spatialspark.fgp.FixedGridPartitionConf
import index.partition.spatialspark.stp.SortTilePartitionConf
import model.entities.Entity
import model.{IM, TileGranularities}
import org.apache.log4j.{LogManager, Logger}
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants.SpatialSparkPartitionMethod
import utils.configuration.Constants.SpatialSparkPartitionMethod.SpatialSparkPartitionMethod

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object Utils extends Serializable {

	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleInt[A](implicit c: ClassTag[Int]): Encoder[Int] = Encoders.scalaInt
	implicit def tuple[String, Int](implicit s: Encoder[String], t: Encoder[Int]): Encoder[(String,Int)] = Encoders.tuple[String,Int](s, t)

	def mbr2pair(x: Geometry, radius: Double, fgConf: FixedGridPartitionConf): Array[(Int, Geometry)] = {
		val results: ArrayBuffer[(Int, Geometry)] = new ArrayBuffer[(Int, Geometry)]()
		val env = x.getEnvelopeInternal
		val mbr = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

		val gridDimX = fgConf.gridDimX
		val gridDimY = fgConf.gridDimY
		val extent = fgConf.extent
		val gridSizeX = (extent.getMaxX - extent.getMinX) / gridDimX.toDouble
		val gridSizeY = (extent.getMaxY - extent.getMinY) / gridDimY.toDouble
		val _xmin: Int = (math.floor((mbr._1 - extent.getMinX) / gridSizeX) max 0).toInt
		val _ymin: Int = (math.floor((mbr._2 - extent.getMinY) / gridSizeY) max 0).toInt
		val _xmax: Int = (math.ceil((mbr._3 - extent.getMinX) / gridSizeX) min gridDimX).toInt
		val _ymax: Int = (math.ceil((mbr._4 - extent.getMinY) / gridSizeY) min gridDimY).toInt

		for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
			val id = j * gridDimX + i
			val pair = (id, x)
			results.append(pair)
		}

		results.toArray
	}

	def getSpatialSparkExtent(source: SpatialRDD[Geometry],
														target: SpatialRDD[Geometry],
														extentString: String): (Double, Double, Double, Double) = {

		extentString match {
			case "" =>
				val temp = source.rawSpatialRDD.rdd
					.map(x => x.getEnvelopeInternal)
					.map(x => (x.getMinX, x.getMaxX, x.getMinY, x.getMaxY))
					.reduce((a, b) => (a._1 min b._1, a._2 max b._2, a._3 min b._3, a._4 max b._4))

				val temp2 = target.rawSpatialRDD.rdd
					.map(x => x.getEnvelopeInternal)
					.map(x => (x.getMinX, x.getMaxX, x.getMinY, x.getMaxY))
					.reduce((a, b) => (a._1 min b._1, a._2 max b._2, a._3 min b._3, a._4 max b._4))

				(temp._1 min temp2._1, temp._2 max temp2._2, temp._3 min temp2._3, temp._4 max temp2._4)

			case _ =>
				(extentString.split(":").apply(0).toDouble, extentString.split(":").apply(1).toDouble,
					extentString.split(":").apply(2).toDouble, extentString.split(":").apply(3).toDouble)
		}
	}

	def getSpatialSparkConfig(method: SpatialSparkPartitionMethod,
														extent: (Double, Double, Double, Double),
														methodConf: String): PartitionConf = {
		method match {
			case SpatialSparkPartitionMethod.STP =>
				val dimX = methodConf.split(":").apply(0).toInt
				val dimY = methodConf.split(":").apply(1).toInt
				val ratio = methodConf.split(":").apply(2).toDouble
				new SortTilePartitionConf(dimX, dimY, new Envelope(extent._1, extent._2, extent._3, extent._4), ratio)

			case SpatialSparkPartitionMethod.BSP =>
				val level = methodConf.split(":").apply(0).toLong
				val ratio = methodConf.split(":").apply(1).toDouble
				new BinarySplitPartitionConf(ratio, new Envelope(extent._1, extent._2, extent._3, extent._4), level)

			case SpatialSparkPartitionMethod.FGP =>
				val dimX = methodConf.split(":").apply(0).toInt
				val dimY = methodConf.split(":").apply(1).toInt
				new FixedGridPartitionConf(dimX, dimY, new Envelope(extent._1, extent._2, extent._3, extent._4))
		}
	}

	val accumulate: Iterator[IM] => (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = imIterator => {
		var totalContains: Int = 0
		var totalCoveredBy: Int = 0
		var totalCovers: Int = 0
		var totalCrosses: Int = 0
		var totalEquals: Int = 0
		var totalIntersects: Int = 0
		var totalOverlaps: Int = 0
		var totalTouches: Int = 0
		var totalWithin: Int = 0
		var verifications: Int = 0
		var qualifiedPairs: Int = 0
		imIterator.foreach { im =>
			verifications += 1
			if (im.relate) {
				qualifiedPairs += 1
				if (im.isContains) totalContains += 1
				if (im.isCoveredBy) totalCoveredBy += 1
				if (im.isCovers) totalCovers += 1
				if (im.isCrosses) totalCrosses += 1
				if (im.isEquals) totalEquals += 1
				if (im.isIntersects) totalIntersects += 1
				if (im.isOverlaps) totalOverlaps += 1
				if (im.isTouches) totalTouches += 1
				if (im.isWithin) totalWithin += 1
			}
		}
		(totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
			totalOverlaps, totalTouches, totalWithin, verifications, qualifiedPairs)
	}

	val accumulateGeoSpark: Iterator[model.geospark.IM] => (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = imIterator => {
		var totalContains: Int = 0
		var totalCoveredBy: Int = 0
		var totalCovers: Int = 0
		var totalCrosses: Int = 0
		var totalEquals: Int = 0
		var totalIntersects: Int = 0
		var totalOverlaps: Int = 0
		var totalTouches: Int = 0
		var totalWithin: Int = 0
		var verifications: Int = 0
		var qualifiedPairs: Int = 0
		imIterator.foreach { im =>
			verifications += 1
			if (im.relate) {
				qualifiedPairs += 1
				if (im.isContains) totalContains += 1
				if (im.isCoveredBy) totalCoveredBy += 1
				if (im.isCovers) totalCovers += 1
				if (im.isCrosses) totalCrosses += 1
				if (im.isEquals) totalEquals += 1
				if (im.isIntersects) totalIntersects += 1
				if (im.isOverlaps) totalOverlaps += 1
				if (im.isTouches) totalTouches += 1
				if (im.isWithin) totalWithin += 1
			}
		}
		(totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
			totalOverlaps, totalTouches, totalWithin, verifications, qualifiedPairs)
	}

	def countAllRelations(imRDD: RDD[IM]): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
		imRDD
			.mapPartitions { imIterator => Iterator(accumulate(imIterator)) }
			.treeReduce({ case (im1, im2) => im1 |+| im2}, 4)

	def countAllRelationsGeoSpark(imRDD: RDD[model.geospark.IM]): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
		imRDD
			.mapPartitions { imIterator => Iterator(accumulateGeoSpark(imIterator)) }
			.treeReduce({ case (im1, im2) => im1 |+| im2}, 4)

	def printPartition(joinedRDD: RDD[(Int, (Iterable[Entity],  Iterable[Entity]))], bordersEnvelope: Array[Envelope], tilesGranularities: TileGranularities): Unit ={
		val c = joinedRDD.map(p => (p._1, (p._2._1.size, p._2._2.size))).sortByKey().collect()
		val log: Logger = LogManager.getRootLogger
		log.info("Printing Partitions")
		log.info("----------------------------------------------------------------------------")
		var pSet = mutable.HashSet[String]()
		c.foreach(p => {
			val zoneStr = bordersEnvelope(p._1).toString
			pSet += zoneStr
			log.info(p._1 + " ->  (" + p._2._1 + ", " + p._2._2 +  ") - " + zoneStr)
		})
		log.info("----------------------------------------------------------------------------")
		log.info("Unique blocks: " + pSet.size)
	}


	def exportCSV(rdd: RDD[(String, String)], path:String): Unit ={
		val spark: SparkSession = SparkSession.builder().getOrCreate()

		val schema = StructType(
			StructField("id1", StringType, nullable = true) ::
				StructField("id2", StringType, nullable = true) :: Nil
		)
		val rowRDD: RDD[Row] = rdd.map(s => new GenericRowWithSchema(Array(s._1, s._2), schema))
		val df = spark.createDataFrame(rowRDD, schema)
		df.write.option("header", "true").csv(path)
	}


	def exportRDF(rdd: RDD[IM], path:String): Unit ={
		val contains = "<http://www.opengis.net/ont/geosparql#sfContains>"
		val coveredBy = "<http://www.opengis.net/ont/geosparql#sfCoverdBy>"
		val covers = "<http://www.opengis.net/ont/geosparql#sfCovers>"
		val crosses = "<http://www.opengis.net/ont/geosparql#sfCrosses>"
		val equals = "<http://www.opengis.net/ont/geosparql#sfEquals>"
		val intersects = "<http://www.opengis.net/ont/geosparql#sfIntersects>"
		val overlaps = "<http://www.opengis.net/ont/geosparql#sfOverlaps>"
		val touches = "<http://www.opengis.net/ont/geosparql#sfTouches>"
		val within = "<http://www.opengis.net/ont/geosparql#sfWithin>"
		rdd.map { im =>
			val sb = new StringBuilder()
			if (im.isContains)
				sb.append("<" + im.getId1 +">" + " " + contains + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isCoveredBy)
				sb.append("<" + im.getId1 +">" + " " + coveredBy + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isCovers)
				sb.append("<" + im.getId1 +">" + " " + covers + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isCrosses)
				sb.append("<" + im.getId1 +">" + " " + crosses + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isEquals)
				sb.append("<" + im.getId1 +">" + " " + equals + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isIntersects)
				sb.append("<" + im.getId1 +">" + " " + intersects + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isOverlaps)
				sb.append("<" + im.getId1 +">" + " " + overlaps + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isTouches)
				sb.append("<" + im.getId1 +">" + " " + touches + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isWithin)
				sb.append("<" + im.getId1 +">" + " " + within + " " + "<" + im.getId2 + ">" + " .\n")
			sb.toString()
		}.saveAsTextFile(path)
	}

	def exportRDFGeoSpark(rdd: RDD[model.geospark.IM], path:String): Unit ={
		val contains = "<http://www.opengis.net/ont/geosparql#sfContains>"
		val coveredBy = "<http://www.opengis.net/ont/geosparql#sfCoverdBy>"
		val covers = "<http://www.opengis.net/ont/geosparql#sfCovers>"
		val crosses = "<http://www.opengis.net/ont/geosparql#sfCrosses>"
		val equals = "<http://www.opengis.net/ont/geosparql#sfEquals>"
		val intersects = "<http://www.opengis.net/ont/geosparql#sfIntersects>"
		val overlaps = "<http://www.opengis.net/ont/geosparql#sfOverlaps>"
		val touches = "<http://www.opengis.net/ont/geosparql#sfTouches>"
		val within = "<http://www.opengis.net/ont/geosparql#sfWithin>"
		rdd.map { im =>
			val sb = new StringBuilder()
			if (im.isContains)
				sb.append("<" + im.getId1 +">" + " " + contains + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isCoveredBy)
				sb.append("<" + im.getId1 +">" + " " + coveredBy + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isCovers)
				sb.append("<" + im.getId1 +">" + " " + covers + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isCrosses)
				sb.append("<" + im.getId1 +">" + " " + crosses + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isEquals)
				sb.append("<" + im.getId1 +">" + " " + equals + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isIntersects)
				sb.append("<" + im.getId1 +">" + " " + intersects + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isOverlaps)
				sb.append("<" + im.getId1 +">" + " " + overlaps + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isTouches)
				sb.append("<" + im.getId1 +">" + " " + touches + " " + "<" + im.getId2 + ">" + " .\n")
			if (im.isWithin)
				sb.append("<" + im.getId1 +">" + " " + within + " " + "<" + im.getId2 + ">" + " .\n")
			sb.toString()
		}.saveAsTextFile(path)
	}
}
