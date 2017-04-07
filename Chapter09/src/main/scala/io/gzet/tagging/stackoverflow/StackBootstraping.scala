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

package io.gzet.tagging.stackoverflow


import io.gzet.tagging.classifier.Classifier
import io.gzet.tagging.html.HtmlHandler
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext}

import scala.collection.mutable
import scala.xml.{Elem, XML}

object StackBootstraping {

  def parse(spark: SparkSession, posts: RDD[String]): DataFrame = {

    import spark.sqlContext.implicits._
    posts filter { line =>
      line.contains("row Id")
    } map { line =>
      val xml = XML.loadString(line)
      (getBody(xml), getTags(xml))
    } filter { case (body, tags) =>
      body.isDefined && tags.isDefined
    } flatMap  { case (body, tags) =>
      tags.get.map(tag => (body.get, tag))
    } toDF("body", "tag")
  }

  private def getBody(xml: Elem): Option[String] = {
    val bodyAttr = xml.attribute("Body")
    if (bodyAttr.isDefined) {
      val html = bodyAttr.get.head.text
      val htmlHandler = new HtmlHandler()
      val content = htmlHandler.parseHtml(html)
      if (content.isDefined) {
        return content.get.body
      }
    }
    None: Option[String]
  }

  private def getTags(xml: Elem): Option[Array[String]] = {
    val tagsAttr = xml.attribute("Tags")
    if (tagsAttr.isDefined) {
      val tagsText = tagsAttr.get.head.text
      val tags = tagsText
        .replaceAll("<", "")
        .replaceAll(">", ",")
        .split(",")
      return Some(tags)
    }
    None: Option[Array[String]]
  }

  def bootstrapNaiveBayes(df: DataFrame, vectorSize: Option[Int]) = {
    val labeledText = df.rdd map { row =>
      val body = row.getString(0)
      val labels = row.getAs[mutable.WrappedArray[String]](1)
      (body, labels.toArray)
    }
    Classifier.train(labeledText)
  }

}

