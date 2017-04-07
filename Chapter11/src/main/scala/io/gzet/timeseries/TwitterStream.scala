package io.gzet.timeseries

import com.google.gson.GsonBuilder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.util.Try

object TwitterStream extends SimpleConfig with Logging {

  def getTwitterStream(ssc: StreamingContext, filters: Seq[String] = Nil) = {
    val builder = new ConfigurationBuilder()
    builder.setOAuthConsumerKey(twitterApiKey)
    builder.setOAuthConsumerSecret(twitterApiSecret)
    builder.setOAuthAccessToken(twitterTokenKey)
    builder.setOAuthAccessTokenSecret(twitterTokenSecret)
    val configuration = builder.build()
    TwitterUtils.createStream(
      ssc,
      Some(new OAuthAuthorization(configuration)),
      filters,
      StorageLevel.MEMORY_ONLY
    )
  }

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("Twitter Extractor")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Minutes(5))

    val twitterStream = getTwitterStream(ssc, args).mapPartitions({ it =>
      val gson = new GsonBuilder().create()
      it map { s =>
        Try(gson.toJson(s))
      }
    })

    twitterStream
      .filter(_.isSuccess)
      .map(_.get)
      .saveAsTextFiles("twitter")

    // Start streaming context
    ssc.start()
    ssc.awaitTermination()

  }

}