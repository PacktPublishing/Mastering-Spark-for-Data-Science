package io.gzet.geomesa.ingest

import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.Collections

import com.google.common.base.Joiner
import com.google.common.collect.Lists
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Geometry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.feature.SchemaException
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object GeomesaAccumuloWrite {

  var LATITUDE_COL_IDX = 39
  var LONGITUDE_COL_IDX = 40
  var DATE_COL_IDX = 1
  var ID_COL_IDX = 0
  var MINIMUM_NUM_FIELDS = 41
  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val dsConf = Map(
    "instanceId" -> "accumulo",
    "zookeepers" -> "127.0.0.1:2181",
    "user" -> "root",
    "password" -> "accumulo",
    "tableName" -> "gdelt")

  val featureName = "event"
  val ingestFile = "file:///gdeltEventsTestFile.csv"

  var attributes = Lists.newArrayList(
    "GLOBALEVENTID:Integer",
    "SQLDATE:Date",
    "MonthYear:Integer",
    "Year:Integer",
    "FractionDate:Float",
    "Actor1Code:String",
    "Actor1Name:String",
    "Actor1CountryCode:String",
    "Actor1KnownGroupCode:String",
    "Actor1EthnicCode:String",
    "Actor1Religion1Code:String",
    "Actor1Religion2Code:String",
    "Actor1Type1Code:String",
    "Actor1Type2Code:String",
    "Actor1Type3Code:String",
    "Actor2Code:String",
    "Actor2Name:String",
    "Actor2CountryCode:String",
    "Actor2KnownGroupCode:String",
    "Actor2EthnicCode:String",
    "Actor2Religion1Code:String",
    "Actor2Religion2Code:String",
    "Actor2Type1Code:String",
    "Actor2Type2Code:String",
    "Actor2Type3Code:String",
    "IsRootEvent:Integer",
    "EventCode:String",
    "EventBaseCode:String",
    "EventRootCode:String",
    "QuadClass:Integer",
    "GoldsteinScale:Float",
    "NumMentions:Integer",
    "NumSources:Integer",
    "NumArticles:Integer",
    "AvgTone:Float",
    "Actor1Geo_Type:Integer",
    "Actor1Geo_FullName:String",
    "Actor1Geo_CountryCode:String",
    "Actor1Geo_ADM1Code:String",
    "Actor1Geo_Lat:Float",
    "Actor1Geo_Long:Float",
    "Actor1Geo_FeatureID:Integer",
    "Actor2Geo_Type:Integer",
    "Actor2Geo_FullName:String",
    "Actor2Geo_CountryCode:String",
    "Actor2Geo_ADM1Code:String",
    "Actor2Geo_Lat:Float",
    "Actor2Geo_Long:Float",
    "Actor2Geo_FeatureID:Integer",
    "ActionGeo_Type:Integer",
    "ActionGeo_FullName:String",
    "ActionGeo_CountryCode:String",
    "ActionGeo_ADM1Code:String",
    "ActionGeo_Lat:Float",
    "ActionGeo_Long:Float",
    "ActionGeo_FeatureID:Integer",
    "DATEADDED:Integer",
    "*geom:Point:srid=4326")

  val featureType: SimpleFeatureType = buildGDELTFeatureType(featureName, attributes)
  val ds = DataStoreFinder.
    getDataStore(dsConf).
    asInstanceOf[AccumuloDataStore].
    createSchema(featureType)

  def createSimpleFeature(value: String): SimpleFeature = {

    val attributes: Array[String] = value.toString.split("\\t", -1)
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    featureBuilder.reset
    val lat: Double = attributes(LATITUDE_COL_IDX).toDouble
    val lon: Double = attributes(LONGITUDE_COL_IDX).toDouble
    if (Math.abs(lat) > 90.0 || Math.abs(lon) > 180.0) {
      // log invalid lat/lon
    }

    val geom: Geometry = geometryFactory.createPoint(new Coordinate(lon, lat))
    val simpleFeature: SimpleFeature = featureBuilder.buildFeature(attributes(ID_COL_IDX))
    var i: Int = 0
    while (i < attributes.length) {
      simpleFeature.setAttribute(i, attributes(i))
      i += 1
    }
    simpleFeature.setAttribute("SQLDATE", formatter.parse(attributes(DATE_COL_IDX)))
    simpleFeature.setDefaultGeometry(geom)
    simpleFeature
  }

  @throws(classOf[SchemaException])
  def buildGDELTFeatureType(featureName: String, attributes: util.ArrayList[String]): SimpleFeatureType = {
    val name = featureName
    val spec = Joiner.on(",").join(attributes)
    val featureType = DataUtilities.createType(name, spec)
    featureType.getUserData.put(Constants.SF_PROPERTY_START_TIME, "SQLDATE")
    featureType
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext(conf.setAppName("Geomesa Ingest"))

    val distDataRDD = sc.textFile(ingestFile)

    val processedRDD: RDD[SimpleFeature] = distDataRDD.mapPartitions { valueIterator =>

      if (valueIterator.isEmpty) {
        Collections.emptyIterator
      }

      //  setup code for SimpleFeatureBuilder
      try {
        val featureType: SimpleFeatureType = buildGDELTFeatureType(featureName, attributes)
        featureBuilder = new SimpleFeatureBuilder(featureType)
      }
      catch {
        case e: Exception => {
          throw new IOException("Error setting up feature type", e)
        }
      }

      valueIterator.map { s =>
        // Processing as before to build the SimpleFeatureType
        val simpleFeature = createSimpleFeature(s)
        if (!valueIterator.hasNext) {
          // cleanup here
        }
        simpleFeature
      }
    }

    GeoMesaSpark.save(processedRDD, dsConf, featureName)
  }
}
