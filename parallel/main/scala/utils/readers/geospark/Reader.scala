package utils.readers.geospark

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import utils.configuration.Constants.FileTypes
import utils.configuration.DatasetConfigurations

object Reader {

    val conf = new SparkConf()
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val spark: SparkSession = SparkSession.getActiveSession.get
    GeoSparkSQLRegistrator.registerAll(spark)

    def read(dc: DatasetConfigurations): SpatialRDD[Geometry] = {
        val extension = dc.getExtension
        val srdd = extension match {
            case FileTypes.CSV =>
                loadDelimitedFile(dc.path, dc.realIdField.getOrElse("0"), dc.geometryField, dc.dateField, ",")
            case FileTypes.TSV =>
                loadDelimitedFile(dc.path, dc.realIdField.getOrElse("0"), dc.geometryField, dc.dateField, "\t")
            case FileTypes.GEOJSON =>
                loadGeoJSON(dc.path, dc.realIdField.getOrElse("id"), dc.dateField)
            case FileTypes.SHP =>
                loadSHP(dc.path, dc.realIdField.getOrElse("id"), dc.dateField)
            case FileTypes.NTRIPLES =>
                loadRdfAsTextual(dc.path, dc.geometryField)
        }
        srdd.rawSpatialRDD = srdd.rawSpatialRDD
          .rdd.filter(g => g.getGeometryType != "GeometryCollection" && g.isValid && !g.isEmpty)
        srdd
    }

    /**
     * Loads a delimited file
     * @param filepath path to the delimited text file
     * @param realIdField instances' unique id
     * @param geometryField geometry field
     * @param dateField date field if exists
     * @param delimiter delimiter
     * @return a spatial RDD
     *
     */
    def loadDelimitedFile(filepath: String, realIdField: String, geometryField: String, dateField: Option[String], delimiter: String): org.datasyslab.geospark.spatialRDD.SpatialRDD[com.vividsolutions.jts.geom.Geometry] ={
//      var inputDF = spark.read.format("csv")
//        .option("delimiter", delimiter)
//        .option("quote", "\"")
//        .option("header", true)
//        .load(filepath)
//        .filter(col(realIdField).isNotNull)
//        .filter(col(geometryField).isNotNull)
//        .filter(!col(geometryField).contains("EMPTY"))
//
//      var query = s"SELECT ST_GeomFromWKT(GEOMETRIES.$geometryField) AS WKT,  GEOMETRIES.$realIdField AS REAL_ID FROM GEOMETRIES".stripMargin
//
//      if (dateField.isDefined) {
//        inputDF = inputDF.filter(col(dateField.get).isNotNull)
//        query = s"SELECT ST_GeomFromWKT(GEOMETRIES.$geometryField) AS WKT,  GEOMETRIES.$realIdField AS REAL_ID, GEOMETRIES.${dateField.get} AS DATE  FROM GEOMETRIES".stripMargin
//      }
//
//      inputDF.createOrReplaceTempView("GEOMETRIES")
//
//      val spatialDF = spark.sql(query)
//      val srdd = new SpatialRDD[Geometry]
//      srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)
//      srdd

      val rawTextRDD = sc.textFile(filepath)
      val idIndex = realIdField.toInt
      val geometryIndex = geometryField.toInt

      val geomRDD: RDD[Geometry] = rawTextRDD.mapPartitions{ p =>
        val reader = new WKTReader()
        val geometryFactory = new GeometryFactory()
        p.map{ line =>
          var id: String = null
          var geom: Geometry = null
          try {
            val tokens = line.split(delimiter)
            val geomText = tokens(geometryIndex)
            val geomTextTrimmed = geomText.replaceAll("^\"|\"$", "")
            id = tokens(idIndex)
            if(geomTextTrimmed.nonEmpty) {
              geom = reader.read(geomTextTrimmed)
            } else geom = geometryFactory.createPolygon(null, null)
          } catch {
            case e: Exception =>
              geom = geometryFactory.createPolygon(null, null)
          }
          geom.setUserData(id)
          geom
        }
      }
      val spatialRDD = new SpatialRDD[Geometry]()
      spatialRDD.rawSpatialRDD = geomRDD
      spatialRDD
    }

    /**
     * Loads RDF as text and not as a Graph model
     * @param filepath path to the delimited text file
     * @param geometryPredicate predicate pointing to Geometries
     * @return a spatial RDD
     */
   def loadRdfAsTextual(filepath: String, geometryPredicate: String): org.datasyslab.geospark.spatialRDD.SpatialRDD[com.vividsolutions.jts.geom.Geometry] = {
        val cleanWKT = (wkt: String) => wkt.replaceAll("<\\S+>\\s?", "").replaceAll("\"", "")
        val rowRDD: RDD[Row] = spark.read.textFile(filepath)
          .rdd.map(s => s.split(" ", 3))
          .filter(s => s(1) == geometryPredicate)
          .map(s => (s(0), cleanWKT(s(2))))
          .filter(s => s._1 != null && s._2 != null && !s._2.isEmpty)
          .filter(s => !s._2.contains("EMPTY"))
          .map(s => Row(s._1, s._2))

        val schema = new StructType()
          .add(StructField("Subject", StringType, nullable = true))
          .add(StructField("WKT", StringType, nullable = true))

        val df = spark.createDataFrame(rowRDD, schema)
        df.createOrReplaceTempView("GEOMETRIES")
        val query = "SELECT ST_GeomFromWKT(GEOMETRIES.WKT),  GEOMETRIES.Subject FROM GEOMETRIES".stripMargin

        val spatialDF = spark.sql(query)
        val srdd = new org.datasyslab.geospark.spatialRDD.SpatialRDD[com.vividsolutions.jts.geom.Geometry]
        srdd.rawSpatialRDD = org.datasyslab.geosparksql.utils.Adapter.toRdd(spatialDF)
        srdd
    }

    /**
     * Loads an ESRI Shapefile
     * @param filepath path to the SHP file
     * @param realIdField instances' unique id
     * @param dateField date field if exists
     * @return a spatial RDD
     */
   def loadSHP(filepath: String, realIdField: String, dateField: Option[String]): org.datasyslab.geospark.spatialRDD.SpatialRDD[com.vividsolutions.jts.geom.Geometry] ={
        val parentFolder = filepath.substring(0, filepath.lastIndexOf("/"))
        val srdd = org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader.readToGeometryRDD(sc, parentFolder)
        adjustUserData(srdd, realIdField, dateField)
    }


    /**
     * Loads a GeoJSON file
     * @param filepath path to the SHP file
     * @param realIdField instances' unique id
     * @param dateField date field if exists
     * @return a spatial RDD
     */
    def loadGeoJSON(filepath: String, realIdField: String, dateField: Option[String]): org.datasyslab.geospark.spatialRDD.SpatialRDD[com.vividsolutions.jts.geom.Geometry] ={
        val srdd = org.datasyslab.geospark.formatMapper.GeoJsonReader.readToGeometryRDD(sc, filepath)
        adjustUserData(srdd, realIdField, dateField)
    }

    /**
     *  Adjust users' data.
     *  Discard all properties except the id and the date if it's requested.
     * @param srdd the input rdd
     * @param realIdField the field of id
     * @param dateField the field of data if it's given
     * @return geometries with only the necessary user data
     */
    def adjustUserData(srdd: org.datasyslab.geospark.spatialRDD.SpatialRDD[com.vividsolutions.jts.geom.Geometry], realIdField: String, dateField: Option[String]): org.datasyslab.geospark.spatialRDD.SpatialRDD[com.vividsolutions.jts.geom.Geometry] ={
        val idIndex = srdd.fieldNames.indexOf(realIdField)
        val rddWithUserData: RDD[com.vividsolutions.jts.geom.Geometry] = dateField match {
            case Some(dateField) =>
                val dateIndex = srdd.fieldNames.indexOf(dateField)
                srdd.rawSpatialRDD.rdd.map { g =>
                    val userData = g.getUserData.toString.split("\t")
                    val id = userData(idIndex)
                    val date = userData(dateIndex)
                    g.setUserData(id + '\t' + date)
                    g
                }
            case _ =>
                srdd.rawSpatialRDD.rdd.map{ g =>
                    val userData = g.getUserData.toString.split("\t")
                    val id = userData(idIndex)
                    g.setUserData(id)
                    g
                }
        }
        srdd.setRawSpatialRDD(rddWithUserData)

        // filter records with valid geometries and ids
        srdd.setRawSpatialRDD(srdd.rawSpatialRDD.rdd.filter(g => ! (g.isEmpty || g == null || g.getUserData.toString == "")))
        srdd
    }
}
