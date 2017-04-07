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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

import scalaz.Scalaz._

trait StringDeduplication extends Serializable {

  def deduplicateWithContext(rdd: RDD[(Long, String)]): RDD[(Long, String)]
  def deduplicate(rdd: RDD[String]): RDD[(String, String)]

  def initialize(rdd: RDD[String]) = {
    rdd map(s => (s, Map(s -> 1)))
  }

  def identityDedup = (rdd: RDD[(String, Map[String, Int])]) => {
    rdd reduceByKey(_ |+| _)
  }

  def getPreferredAlternative(rdd: RDD[(String, Map[String, Int])]) = {
    rdd flatMap { case (key, tf) =>
      val bestName = tf.toSeq.sortBy(_._2).reverse.head._1
      tf.keySet map(_ -> bestName)
    }
  }

  def stringDedup = (rdd: RDD[(String, Map[String, Int])], stopWords: Set[String]) => {
    rdd map { case (name, others) =>
      (clean(name, stopWords), others)
    } reduceByKey(_ |+| _)
  }

  private def clean(name: String, stopWords: Set[String]) = {
    StringUtils.stripAccents(name)
      .split("\\W+")
      .map(_.trim)
      .filter({ case part => !stopWords.contains(part.toLowerCase()) })
      .mkString(" ")
      .split("(?<=[a-z])(?=[A-Z])")
      .mkString(" ")
      .toLowerCase()
      .split("[^a-z]")
      .map(_.trim)
      .mkString(" ")
  }

}
