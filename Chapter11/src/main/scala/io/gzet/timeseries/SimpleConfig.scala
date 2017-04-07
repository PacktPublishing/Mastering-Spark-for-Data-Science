package io.gzet.timeseries

import java.util.UUID

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._
import scala.util.Try

trait SimpleConfig {

  lazy val conf = ConfigFactory.load()
  lazy val batchSize = Try(conf.getInt("batchSize")).getOrElse(10)
  lazy val windowSize = Try(conf.getInt("windowSize")).getOrElse(60)
  lazy val checkpointDir = Try(conf.getString("checkpoint")).getOrElse(s"file:///tmp/${UUID.randomUUID()}")

  lazy val timely = conf.getConfig("timely")
  lazy val timelyHost = Try(timely.getString("ip")).getOrElse("localhost")
  lazy val timelyPort = Try(timely.getInt("port")).getOrElse(54321)

  lazy val twitter = conf.getConfig("twitter")
  lazy val twitterFilter = twitter.getStringList("tags").toList
  lazy val twitterApiKey = twitter.getString("apiKey")
  lazy val twitterApiSecret = twitter.getString("apiSecret")
  lazy val twitterTokenKey = twitter.getString("tokenKey")
  lazy val twitterTokenSecret = twitter.getString("tokenSecret")

}
