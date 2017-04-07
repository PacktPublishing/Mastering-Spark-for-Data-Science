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

package io.gzet.tagging.gdelt

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.ConfigFactory
import io.gzet.tagging.classifier.Classifier
import io.gzet.tagging.html.HtmlHandler
import io.gzet.tagging.html.HtmlHandler.Content
import org.apache.spark.Accumulator
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.LongAccumulator
import org.elasticsearch.spark._

class GdeltTagger() extends Serializable {

  val config = ConfigFactory.load().getConfig("io.gzet.kappa")
  val isoSdf = "yyyy-MM-dd HH:mm:ss"
  val esIndex = config.getString("gdeltIndex")
  val vectorSize = config.getInt("vectorSize")
  val minProba = config.getDouble("minProba")

  def predict(gdeltStream: DStream[String], batchId: LongAccumulator) = {

    // Extract HTML content
    val gdeltContent = fetchHtmlContent(gdeltStream)

    // Predict each RDD
    gdeltContent foreachRDD { batch =>

      batch.cache()
      val count = batch.count()

      if (count > 0) {

        if (Classifier.model.isDefined) {
          val labels = Classifier.model.get.labels

          // Predict HashTags using latest Twitter model
          val textRdd = batch.map(_.body.get)
          val predictions = Classifier.predictProbabilities(textRdd)
          val taggedGdelt = batch.zip(predictions) map { case (content, probabilities) =>
            val validLabels = probabilities filter { case (label, probability) =>
              probability > minProba
            }

            val labels = validLabels.toSeq
              .sortBy(_._2)
              .reverse
              .map(_._1)

            (content, labels)
          }

          // Saving articles to Elasticsearch
          taggedGdelt map { case (content, hashTags) =>
            gdeltToJson(content, hashTags.toArray)
          } saveToEs esIndex

        } else {

          // Saving articles to Elasticsearch
          batch map { content =>
            gdeltToJson(content, Array())
          } saveToEs esIndex
        }

      }

      batch.unpersist(blocking = false)
    }
  }

  private def gdeltToJson(content: Content, hashTags: Array[String]) = {
    val sdf = new SimpleDateFormat(isoSdf)
    Map(
      "time" -> sdf.format(new Date()),
      "body" -> content.body.get,
      "url" -> content.url,
      "tags" -> hashTags,
      "title" -> content.title
    )
  }

  private def fetchHtmlContent(urlStream: DStream[String]) = {
    urlStream.map(_ -> 1).groupByKey().map(_._1) mapPartitions { urls =>
      val sdf = new SimpleDateFormat(isoSdf)
      val htmlHandler = new HtmlHandler()
      val goose = htmlHandler.getGooseScraper
      urls map { url =>
        htmlHandler.fetchUrl(goose, url, sdf)
      }
    } filter { content =>
      content.isDefined &&
        content.get.body.isDefined &&
        content.get.body.get.length > 255
    } map { content =>
      content.get
    }
  }
}
