package io.gzet.geomesa

import java.text.SimpleDateFormat

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{udf, window, collect_list, monotonically_increasing_id}

import scala.collection.mutable.WrappedArray

object PredictOilPrice {

  // the oil prices input data
  val inputfile = "Brent-oil-prices-2016.csv"

  // the OilPriceFunc output file
  val outputfile = "Brent-2016-Weekly-Change"

  // the CAMEO data
  val cameofile = "CAMEO_codes.txt"

  // the output from GeomesaAccumuloRead - assumes this file has been created from GeomesaAccumuloRead.scala
  val sourcefile = "gdeltAttrRDD2016"


  def main(args: Array[String]) {
    // ------------- the data science part

    val spark = SparkSession
      .builder()
      .appName("PredictOilPrice")
      .config("spark.master", "local")
      .getOrCreate()

    // use this to load saved data
    val gdeltAttrRDDListString = spark.sparkContext.objectFile[List[String]](sourcefile)
    val gdeltAttrRDD = gdeltAttrRDDListString.map(a => Row(a(0), a(1), a(2), a(3), a(4)))

    // create a Map to hold the CAMEO codes and their references
    var cameoMap = scala.collection.mutable.Map[String, String]()

    val linesRDD = spark.sparkContext.textFile(cameofile)
    linesRDD.collect.foreach(line => {
      val splitsArr = line.split("\t")
      cameoMap += (splitsArr(0) -> splitsArr(1).
        replaceAll("[^A-Za-z0-9 ]", ""))
    })

    // actor1 + eventcode + actor2
    // output according to number of articles
    val bagOfWordsRDD = gdeltAttrRDD.map(f => Row(
      f.get(0),
      f.get(1).toString.replaceAll("\\s", "").
        toLowerCase + " " + cameoMap(f.get(3).toString).
        toLowerCase + " " + f.get(2).toString.replaceAll("\\s", "").
        toLowerCase)
    )

    val gdeltSentenceStruct = StructType(Array(
      StructField("Date", StringType, true),
      StructField("sentence", StringType, true)
    ))

    val gdeltSentenceDF = spark.createDataFrame(bagOfWordsRDD, gdeltSentenceStruct)
    gdeltSentenceDF.show(10, false)

    // group dates into 7 day blocks and merge the sentences - runs friday to thursday

    val windowAgg = gdeltSentenceDF.groupBy(window(gdeltSentenceDF.col("Date"), "7 days", "7 days", "1 day"))
    val sentencesDF = windowAgg.agg(collect_list("sentence") as "sentenceArray")
    // add new column with UDF transforming sentences array to string
    val convertWrappedArrayToStringUDF = udf { (array: WrappedArray[String]) =>
      array.mkString(" ")
    }

    val dateConvertUDF = udf { (date: String) =>
      new SimpleDateFormat("yyyy-MM-dd").
        format(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").
          parse(date))
    }

    val aggSentenceDF = sentencesDF.
      withColumn("text", convertWrappedArrayToStringUDF(sentencesDF("sentenceArray"))).
      withColumn("commonFriday", dateConvertUDF(sentencesDF("window.end")))

    // create the oilPriceChangeDF
    OilPriceFunc.createOilPriceDF(inputfile, outputfile, spark)
    val oilPriceChangeDF = spark.
      read.
      option("inferSchema", "true").
      csv(outputfile)

    // merge this DF to the oilProceChangeDF
    val changeJoinDF = aggSentenceDF.
      drop("window").
      drop("sentenceArray").
      join(oilPriceChangeDF, Seq("commonFriday")).
      withColumn("id", monotonically_increasing_id)

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
    val splitDS = changeJoinDF.randomSplit(Array(0.60, 0.40))
    val (trainingDF, testDF) = (splitDS(0), splitDS(1))

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
