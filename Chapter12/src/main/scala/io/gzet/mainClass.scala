package io.gzet

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{udf, window, max, expr}

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

object mainClass {

  val inputfile = "brent-oil-prices.csv"	// /Users/uktpmhallett/Downloads/OIL/brent-oil-prices.csv

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample")
      .config("spark.master", "local")
      .getOrCreate()

    def convert(date:String) : String = {
      val dt = new SimpleDateFormat("dd/MM/yyyy").parse(date)
      new SimpleDateFormat("yyyy-MM-dd").format(dt)
    }

    def printWindow(windowDF: DataFrame, aggCol:String) ={
      windowDF.sort("window.start").
        select("window.start", "window.end", s"$aggCol").show(truncate = false)
    }

    val oilPriceDF = spark
        .read
        .option("header","true")
        .option("inferSchema", "true")
        .csv(inputfile)

    val convertDateUDF = udf {(Date: String) => convert(Date)}

    val oilPriceDatedDF = oilPriceDF.withColumn("DATE", convertDateUDF(oilPriceDF("DATE")))

    // offset to start at beginning of week
    val windowDF = oilPriceDatedDF.groupBy(window(oilPriceDatedDF.col("DATE"),"7 days", "7 days", "4 days"))

    val hlc = new HighLowCalc

    spark.udf.register("hlc", hlc)

    val highLowDF = windowDF.agg(expr("hlc(DATE,PRICE) as highLow"))
    highLowDF.show(20, false)

    // compare lines in table
    // use lag to get last row stuff

    // TODO: Window needs partitioning
    val sortedWindow = Window.orderBy("window.start")

    val lagCol = lag(col("highLow"), 1).over(sortedWindow)

    val highLowPrevDF = highLowDF.withColumn("highLowPrev", lagCol)
    highLowPrevDF.show(20, false)

    // get +1 -1 values
    val simpleTrendFunc = udf{(currentHigh : Double, currentLow : Double, prevHigh : Double, prevLow : Double) =>
      {
        (((currentHigh - prevHigh) compare 0).signum + ((currentLow - prevLow) compare 0).signum compare 0).signum
      }
    }

    //(10.0 compare 0).signum
    val simpleTrendDF = highLowPrevDF.withColumn("sign", simpleTrendFunc(highLowPrevDF("highLow.HighestHighPrice"),
      highLowPrevDF("highLow.LowestLowPrice"),
      highLowPrevDF("highLowPrev.HighestHighPrice"),
      highLowPrevDF("highLowPrev.LowestLowPrice")
    ))
    
//    simpleTrendDF.show(20, false)

    //find the reversals
    //for each window check if this signum is opposite to next signum, if so mark this as reversal
    val lagSignCol = lag(col("sign"), 1).over(sortedWindow)
    val lagSignColDF = simpleTrendDF.withColumn("signPrev", lagSignCol)

//    case class reversal(date: String, price: Double)

    val reversalUDF = udf((currentSign : Int, prevSign : Int, prevHighPrice : Double, prevHighDate : String, prevLowPrice : Double, prevLowDate : String) =>
        (currentSign compare prevSign).signum match {
          case 0 => null
          case -1 => (prevHighDate, prevHighPrice)
          case 1 => (prevLowDate, prevLowPrice)
        })

    val reversalsDF = lagSignColDF.withColumn("reversals", reversalUDF(lagSignColDF("sign"),
      lagSignColDF("signPrev"),
      lagSignColDF("highLowPrev.HighestHighPrice"),
      lagSignColDF("highLowPrev.HighestHighDate"),
      lagSignColDF("highLowPrev.LowestLowPrice"),
      lagSignColDF("highLowPrev.LowestLowDate")
    ))
    
//	   reversalsDF.show(20, truncate = false)

    // insert timeseriesID
    reversalsDF.select("window.start")

    // FHLS

    val lookup = Map("_1" -> "reversalDate", "_2" -> "reversalPrice")
    val fhlsSelectDF = reversalsDF.select(
      "window.start",
      "highLow.firstPrice",
      "highLow.HighestHighPrice",
      "highLow.LowestLowPrice",
      "highLow.secondPrice",
      "highLow.HighestHighDate",
      "highLow.LowestLowDate",
      "window.end",
      "reversals._1",
      "reversals._2")

    val fhlsDF = fhlsSelectDF.select(fhlsSelectDF.columns.map(c => col(c).as(lookup.getOrElse(c, c))):_* )//.withColumnRenamed("_1", "reversalDate")
    fhlsDF.orderBy(asc("start")).show(20, false)

    fhlsDF.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
//      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("/Users/uktpmhallett/Documents/fhls");

    // find 0s, insert rows
    // use simpleTrendDF


    // code for stackable
    val newColumnNames = Seq("DATE", "PRICE")

    val highLowHighestDF = simpleTrendDF.select("highLow.HighestHighDate", "highLow.HighestHighPrice").toDF(newColumnNames:_*)

    val highLowLowestDF = simpleTrendDF.select("highLow.LowestLowDate", "highLow.LowestLowPrice").toDF(newColumnNames:_*)

    val stackedDF = highLowHighestDF.union(highLowLowestDF)

    stackedDF.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("stackData.csv");

//    val withoutCurrency = spark.createDataFrame(highLowDF.rdd.map(x => {
//      RowFactory.create(x.getAs("window")., x.getAs("highLow"))
//    }), highLowDF.schema)
  }
}
