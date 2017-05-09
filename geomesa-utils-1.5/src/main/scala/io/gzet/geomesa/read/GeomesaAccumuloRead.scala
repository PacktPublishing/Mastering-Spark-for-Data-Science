package io.gzet.geomesa.read

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.compute.spark.GeoMesaSpark

import scala.collection.JavaConversions._

object GeomesaAccumuloRead {

  // written for current version of Geomesa - Spark1.5
  // spark-1.5.0/bin/spark-submit --class io.gzet.geomesa.GeomesaAccumuloRead geomesa-utils-15-1.0.jar

  // specify the params for the datastore
  val params = Map(
    "instanceId" -> "accumulo",
    "zookeepers" -> "127.0.0.1:2181",
    "user" -> "root",
    "password" -> "accumulo",
    "tableName" -> "gdelt")

  // matches the params in the datastore loading code
  val typeName = "event"
  val geom = "geom"
  val date = "SQLDATE"
  val actor1 = "Actor1Name"
  val actor2 = "Actor2Name"
  val eventCode = "EventCode"
  val numArticles = "NumArticles"

  val bbox = "34.515610, -21.445313, 69.744748, 36.914063"
  val during = "2016-01-01T00:00:00.000Z/2016-12-30T00:00:00.000Z"

  val filter = s"bbox($geom, $bbox) AND $date during $during"

  // writes to HDFS - prepend with file:/// for local filesystem
  val outputFile = "gdeltAttrRDD2016"

  def main(args: Array[String]) {

    // ------------- read from GeoMesa store

    // Get a handle to the data store
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

    // Construct a CQL query to filter by bounding box
    val q = new Query(typeName, ECQL.toFilter(filter))

    // Configure Spark
    val sc = new SparkContext(GeoMesaSpark.init(new SparkConf(true), ds))

    // Create an RDD from a query
    val simpleFeaureRDD = GeoMesaSpark.rdd(new Configuration, sc, params, q)

    // Convert RDD[SimpleFeature] to RDD[Row] for Dataframe creation below
    val gdeltAttrRDD = simpleFeaureRDD.mapPartitions { iter =>
      val df = new SimpleDateFormat("yyyy-MM-dd")
      val ff = CommonFactoryFinder.getFilterFactory2
      val dt = ff.property(date)
      val a1n = ff.property(actor1)
      val a2n = ff.property(actor2)
      val ec = ff.property(eventCode)
      val na = ff.property(numArticles)
      iter.map { f =>
        List(
          df.format(dt.evaluate(f).asInstanceOf[java.util.Date]),
          a1n.evaluate(f),
          a2n.evaluate(f),
          ec.evaluate(f),
          na.evaluate(f)
        )
      }
    }

    ds.dispose

    // use this to save the GeoMesa data
    gdeltAttrRDD.saveAsObjectFile(outputFile)

  }
}


