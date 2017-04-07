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

package io.gzet.tagging

import com.typesafe.config.ConfigFactory
import io.gzet.tagging.gdelt.GdeltTagger
import io.gzet.tagging.twitter.TwitterHIS
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object KappaTagging {

  final val config = ConfigFactory.load().getConfig("io.gzet.kappa")
  final val esNodes = config.getString("esNodes")
  final val batchSize = config.getInt("batchSize")

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("GDELT Kappa tagging")
    val ssc = new StreamingContext(sparkConf, Seconds(batchSize))
    val sc = ssc.sparkContext

    // Create a counter that can be shared accross batches
    val batchId = sc.longAccumulator("GZET")

    val twitterStream = createTwitterStream(ssc, Array[String]())
    val twitterProcessor = new TwitterHIS()
    twitterProcessor.train(twitterStream, batchId)

    val gdeltStream = createGdeltStream(ssc)
    val gdeltProcessor = new GdeltTagger()
    gdeltProcessor.predict(gdeltStream, batchId)

    ssc.start()
    ssc.awaitTermination()
  }

  private def createTwitterStream(ssc: StreamingContext, filters: Array[String]): DStream[Status] = {
    TwitterUtils.createStream(
      ssc,
      getTwitterConfiguration,
      filters
    )
  }

  private def getTwitterConfiguration = {
    val builder = new ConfigurationBuilder()
    builder.setOAuthConsumerKey(config.getString("apiKey"))
    builder.setOAuthConsumerSecret(config.getString("apiSecret"))
    builder.setOAuthAccessToken(config.getString("tokenKey"))
    builder.setOAuthAccessTokenSecret(config.getString("tokenSecret"))
    val configuration = builder.build()
    Some(new OAuthAuthorization(configuration))
  }

  private def createGdeltStream(ssc: StreamingContext) = {
    val topics = Map(
      config.getString("kafkaTopic") -> config.getInt("kafkaTopicPartition")
    )
    KafkaUtils.createStream(
      ssc,
      config.getString("zkQuorum"),
      config.getString("kafkaGroupId"),
      topics
    ).map(_._2)
  }

}
