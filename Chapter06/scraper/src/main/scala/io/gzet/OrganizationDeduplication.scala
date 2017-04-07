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

import org.apache.spark.rdd.RDD
import scalaz.Scalaz._

class OrganizationDeduplication() extends StringDeduplication with Serializable {

  override def deduplicateWithContext(rdd: RDD[(Long, String)]): RDD[(Long, String)] = {
    val nameRdd = rdd.values
    val replacementRdd = deduplicate(nameRdd)
    replacementRdd.rightOuterJoin(rdd.map(_.swap)).map({case (name, (betterName, rddId)) =>
      (rddId, betterName.getOrElse(name))
    })
  }

  override def deduplicate(rdd: RDD[String]): RDD[(String, String)] = {
    val dedup1 = initialize(rdd)
    val dedup2 = identityDedup(dedup1)
    val dedup3 = stringDedup(dedup2, OrganizationDeduplication.stopWords)
    val dedup4 = initialsDedup(dedup3)
    getPreferredAlternative(dedup4)
  }

  def initialsDedup = (rdd: RDD[(String, Map[String, Int])]) => {

    rdd map { case (name, others) =>
      (getInitials(name), (name, others))
    } reduceByKey { case ((n1, m1), (n2, m2)) =>
      if (m1.values.sum > m2.values.sum) {
        (n1, m1 |+| m2)
      } else {
        (n2, m1 |+| m2)
      }
    } values
  }

  private def getInitials(name: String) = {
    val parts = name.split("\\s")
    if(parts.length > 1) {
      parts.map(_.toCharArray).filter(_.nonEmpty).map(_.head).mkString("")
    } else {
      name
    }
  }
}

object OrganizationDeduplication {

  val stopWords = Set("the", "of", "at", "in", "and", "ltd", "limited", "inc", "incorporated", "sa", "sarl", "plc")

}
