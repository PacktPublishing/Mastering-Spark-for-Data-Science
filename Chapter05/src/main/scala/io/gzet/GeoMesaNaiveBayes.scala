/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gzet

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.spark
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.compute.spark.GeoMesaSpark


import org.apache.spark.ml.feature.{IDF, HashingTF}


import scala.collection.JavaConversions._
import scala.collection.mutable.WrappedArray

object GeoMesaNaiveBayes {

  // specify the params for the datastore
  val params = Map(
    "instanceId" -> "accumulo",
    "zookeepers" -> "127.0.0.1:2181",
    "user"       -> "root",
    "password"   -> "accumulo",
    "tableName"  -> "gdelt")

  // matches the params in the datastore loading code
  val typeName      = "event"
  val geom          = "geom"
  val date          = "SQLDATE"
  val actor1        = "Actor1Name"
  val actor2        = "Actor2Name"
  val eventCode     = "EventCode"
  val numArticles   = "NumArticles"

  val bbox   = "34.515610, -21.445313, 69.744748, 36.914063"
  val during = "2016-01-01T00:00:00.000Z/2016-12-30T00:00:00.000Z"

  val filter = s"bbox($geom, $bbox) AND $date during $during"

  // the oil prices input data
  val inputfile = getClass.getResource("/brent-oil-prices-2016.csv")      // "file:///Users/uktpmhallett/Documents/OIL/brent-oil-prices-2016.csv"

  // the OilPriceFunc output file
  val outputfile = getClass.getResource("/Bremt-2016-Weekly-Change")      // "file:///Users/uktpmhallett/Documents/Bremt-2016-Weekly-Change"

  // the CAMEO data
  val cameofile = getClass.getResource("/CAMEO_codes.txt")                // "file:///Users/uktpmhallett/Documents/CAMEO_codes.txt"

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
        Row(// List
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
    // gdeltAttrRDD.saveAsObjectFile("gdeltAttrRDD2016")

    // ------------- the data science part

    // use this to load saved data
    // val gdeltAttrRDDListString = sc.objectFile[List[String]]("gdeltAttrRDD2016")
    // val gdeltAttrRDD = gdeltAttrRDDListString.map(a => Row(a(0), a(1), a(2), a(3), a(4)))

    // create a Map to hold the CAMEO codes and their references
    var cameoMap = scala.collection.mutable.Map[String, String]()

    val linesRDD = sc.textFile(cameofile)
    linesRDD.collect.foreach(line => {
      val splitsArr = line.split("\t")
      cameoMap += (splitsArr(0) -> splitsArr(1).
        replaceAll("[^A-Za-z0-9 ]", ""))
     })

    // actor1 + eventcode + actor2
    // output according to number of articles
    val bagOfWordsRDD = gdeltAttrRDD.map(f => Row(
      f.get(0),
      f.get(1).toString.replaceAll("\\s","").
        toLowerCase + " " + cameoMap(f.get(3).toString).
        toLowerCase + " " + f.get(2).toString.replaceAll("\\s","").
        toLowerCase)
    )

    val gdeltSentenceStruct = StructType(Array(
      StructField("Date", StringType, true),
      StructField("sentence", StringType, true)
    ))

    val gdeltSentenceDF = spark.createDataFrame(bagOfWordsRDD, gdeltSentenceStruct)
    gdeltSentenceDF.show(10, false)

    // group dates into 7 day blocks and merge the sentences - runs friday to thursday

    val windowAgg = gdeltSentenceDF.groupBy(window(gdeltSentenceDF.col("Date"),"7 days", "7 days", "1 day"))
    val sentencesDF = windowAgg.agg(collect_list("sentence") as "sentenceArray")
    // add new column with UDF transforming sentences array to string
    val convertWrappedArrayToStringUDF = udf {(array: WrappedArray[String]) =>
      array.mkString(" ")
    }

    val dateConvertUDF = udf {(date: String) =>
      new SimpleDateFormat("yyyy-MM-dd").
        format(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").
          parse(date))
      }

    val aggSentenceDF = sentencesDF.
      withColumn("text", convertWrappedArrayToStringUDF(sentencesDF("sentenceArray"))).
      withColumn("commonFriday", dateConvertUDF(sentencesDF("window.end")))

    // create the oilPriceChangeDF
    OilPriceFunc.createOilPriceDF(inputfile, outputfile)
    val oilPriceChangeDF = spark.
      read.
      option("inferSchema", "true").
      csv(outputfile)

    // merge this DF to the oilProceChangeDF
    val changeJoinDF = aggSentenceDF.
      drop("window").
      drop("sentenceArray").
      join(oilPriceChangeDF, Seq("commonFriday")).
      withColumn("id", monotonicallyIncreasingId)

    // create the stages
    val tokenizer = new Tokenizer().
      setInputCol("text").
      setOutputCol("words")
    val hashingTF = new HashingTF().
      setNumFeatures(10000).
      setInputCol(tokenizer.getOutputCol).
      setOutputCol("rawFeatures")
    val idf = new IDF().
      setInputCol(hashingTF.getOutputCol).
      setOutputCol("features")
    val nb = new NaiveBayes()

    // create the pipeline
    val pipeline = new Pipeline().
      setStages(Array(tokenizer, hashingTF, idf, nb))

    // split the gdeltAttrDF into training and test sets
    val splitDS = changeJoinDF.randomSplit(Array(0.60,0.40))
    val (trainingDF,testDF) = (splitDS(0),splitDS(1))

    // train the model
    val model = pipeline.fit(trainingDF)

    // show the model accuracy
    model.transform(testDF).
      select("id", "prediction", "label").
      collect().
      foreach { case Row(id: Long, prediction: Double, label: Double) =>
        println(s"$id --> prediction=$prediction --> should be: $label")
      }
  }
}
