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

package io.gzet.tagging.twitter

import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.ConfigFactory
import io.gzet.tagging.classifier.Classifier
import io.gzet.tagging.html.HtmlHandler
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.{LongAccumulator, AccumulatorV2}
import org.apache.spark.{Accumulator, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.Metadata._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import twitter4j.Status

import scala.collection.JavaConversions._
import scala.util.Try

class TwitterHIS() extends Serializable {

  val config = ConfigFactory.load().getConfig("io.gzet.kappa")
  val isoSdf = "yyyy-MM-dd HH:mm:ss"
  val windowSize = config.getInt("windowSize")
  val ttl = config.getString("ttl")
  val esIndex = config.getString("twitterIndex")
  val modelOutputDir = config.getString("modelOutputDir")
  val minHashTagLength = config.getInt("minHashTagLength")
  val topHashTags = config.getInt("topHashTags")
  val untrustedSources = config.getStringList("untrustedHosts").toSet

  def train(stream: DStream[Status], batchId: LongAccumulator) = {

    val spark = SparkSession.builder()
      .appName("MechanicalTurk")
      .getOrCreate()

    val sc = spark.sparkContext
    val untrustedSources = sc.broadcast(this.untrustedSources)

    // Only keep english tweets
    val twitterStream = stream.filter(_.getUser.getLang == "en")

    // Extract HashTags and URLs
    val labeledUrls = getLabeledUrls(twitterStream)

    // Count HashTags within a time window
    val twitterTrend = getTrends(twitterStream, batchId)

    // Join articles with HashTag and only keep valid HashTags / articles
    val labeledTrendUrls = join(twitterTrend, labeledUrls)

    // Only keep valid urls
    val labeledTrendExpandedUrls = expandUrls(labeledTrendUrls, untrustedSources)

    // Download articles for these trendy tweets articles
    val trendTwitterArticles = fetchHtmlContent(labeledTrendExpandedUrls)

    trendTwitterArticles foreachRDD { batch =>

      batchId.add(1)
      val sc = batch.sparkContext
      batch.cache()
      val bCount = batch.count()

      if (bCount > 0) {

        // Access all online records from previous windows
        val window = getOnlineRecords(sc)
        window.cache()
        window.count()

        // Save current batch to elasticsearch
        saveBatch(batch, batchId.value)

        // Build our full training Set
        val trainingSet = batch.union(window)

        // Train and save a new model
        Classifier.train(trainingSet).save(sc, s"$modelOutputDir/${batchId.value}")
        window.unpersist(blocking = false)
      }

      batch.unpersist(blocking = false)

    }
  }

  private def getOnlineRecords(sc: SparkContext) = {
    sc.esJsonRDD(esIndex).values map { jsonStr =>
      implicit val format = DefaultFormats
      val json = parse(jsonStr)
      val tags = (json \ "tags").extract[Array[String]]
      val body = (json \ "body").extract[String]
      (body, tags)
    }
  }

  private def saveBatch(batch: RDD[(String, Array[String])], batchId: Long) = {
    batch mapPartitions { it =>
      val sdf = new SimpleDateFormat(isoSdf)
      it map { case (content, tags) =>
        val data = Map(
          "time" -> sdf.format(new Date()),
          "batch" -> batchId,
          "body" -> content,
          "tags" -> tags
        )
        (Map(TTL -> ttl), data)
      }
    } saveToEsWithMeta esIndex
  }

  private def getTrends(twitterStream: DStream[Status], batchId: LongAccumulator): DStream[(String, Int)] = {

    twitterStream flatMap { tweet =>
      extractTags(tweet.getText)
    } map (_ -> 1) reduceByKeyAndWindow(_ + _, Seconds(windowSize))

  }

  private def join(twitterTrend: DStream[(String, Int)], labeledUrls: DStream[(String, Array[String])]): DStream[(String, Array[String])] = {

    labeledUrls.transformWith(twitterTrend, (labeledUrls: RDD[(String, Array[String])], twitterTrend: RDD[(String, Int)]) => {

      val sc = twitterTrend.sparkContext
      val leaderBoard = sc.broadcast(twitterTrend
        .sortBy(_._2, ascending = false)
        .take(topHashTags)
        .map(_._1)
      )

      labeledUrls flatMap { case (url, tags) =>
        tags map (tag => (url, tag))
      } filter { case (url, tag) =>
        leaderBoard.value.contains(tag)
      } groupByKey() mapValues (_.toArray.distinct)

    })
  }

  private def getLabeledUrls(twitterStream: DStream[Status]) = {
    twitterStream flatMap { tweet =>
      val tags = extractTags(tweet.getText)
      val urls = extractUrls(tweet.getText)
      urls map { url =>
        (url, tags)
      }
    }
  }

  private def extractTags(tweet: String) = {
    StringUtils.stripAccents(tweet.toLowerCase())
      .split("\\s")
      .filter({ word =>
        word.startsWith("#") &&
          word.length > minHashTagLength &&
          word.matches("#[a-z]+")
      })
  }

  private def extractUrls(tweet: String) = {
    tweet.split("\\s")
      .filter(_.startsWith("http"))
      .map(_.trim)
      .filter(url => Try(new URL(url)).isSuccess)
  }

  private def expandUrls(twitterStream: DStream[(String, Array[String])], untrustedSources: Broadcast[Set[String]]) = {
    twitterStream map { case (url, tags) =>
      (HtmlHandler.expandUrl(url), tags)
    } filter { case (url, tags) =>
      !untrustedSources.value.contains(url)
    }
  }

  private def fetchHtmlContent(validLabeledUrls: DStream[(String, Array[String])]) = {
    validLabeledUrls.reduceByKey(_ ++ _.distinct) mapPartitions { it =>
      val htmlFetcher = new HtmlHandler()
      val goose = htmlFetcher.getGooseScraper
      val sdf = new SimpleDateFormat("yyyyMMdd")
      it map { case (url, tags) =>
        val content = htmlFetcher.fetchUrl(goose, url, sdf)
        (content, tags)
      } filter { case (contentOpt, tags) =>
        contentOpt.isDefined &&
          contentOpt.get.body.isDefined &&
          contentOpt.get.body.get.split("\\s+").length >= 500
      } map { case (contentOpt, tags) =>
        (contentOpt.get.body.get, tags)
      }
    }
  }
}
