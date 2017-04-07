package io.gzet.timeseries

import io.gzet.timeseries.twitter.Twitter._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

import scala.language.postfixOps

object TwitterBatch extends SimpleConfig with Logging {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("Twitter Extractor")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val twitterJsonRDD = sc.parallelize(sc.textFile("file:///Users/antoine/CHAPTER/twitter-trump").take(500), 1)
    val tweetRDD = twitterJsonRDD mapPartitions analyzeJson cache()
    val tweetDF = tweetRDD.toDF().cache()
    tweetDF.count

    tweetDF.write.json("file:///Users/antoine/CHAPTER/twitter-trump-sentiment")
    tweetDF.saveToEs("gzet/twitter")

  }

}