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

import java.net.URL

import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.spark.rdd.RDD

import scala.collection
import scalaz.Scalaz._

class NameDeduplication() extends StringDeduplication with Serializable {

  lazy val allNicknames = NameDeduplication.loadNicknames(this.getClass.getResource("/nicknames.dat"))

  override def deduplicateWithContext(rdd: RDD[(Long, String)]): RDD[(Long, String)] = {
    val contextDedupRdd = contextDedup(rdd)
    val nameRdd = contextDedupRdd.values
    val replacementRdd = deduplicate(nameRdd)
    replacementRdd.rightOuterJoin(contextDedupRdd.map(_.swap)).map({case (name, (betterName, rddId)) =>
      (rddId, betterName.getOrElse(name))
    })
  }

  override def deduplicate(rdd: RDD[String]): RDD[(String, String)] = {
    val dedup1 = initialize(rdd)
    val dedup2 = identityDedup(dedup1)
    val dedup3 = stringDedup(dedup2, NameDeduplication.stopWords)
    val dedup4 = metaphoneDedup(dedup3)
    getPreferredAlternative(dedup4)
  }

  def contextDedup(rdd: RDD[(Long, String)]) = {

    val replacedContextRdd = rdd.groupByKey() mapValues { case it =>

      val persons = it.toList

      // Find people sharing same part of name
      val contextCandidates: Map[String, List[String]] = persons.flatMap({ name =>
        val parts = name.split("\\W+").filter(_.length > 3)
        for(part <- parts) yield (part, name)
      }).groupBy(_._1).mapValues(_.map(_._2).toList).filter(_._2.distinct.size == 2)

      // Find the best alternative (obama => barack obama)
      val replacedMap = contextCandidates map { case (part, candidates) =>
        val orderedCandidates = candidates.toList.sortBy(_.length).reverse
        (part, orderedCandidates)
      } flatMap { case (part, candidates) =>
        val preferredName = candidates.head
        candidates map { altName =>
          (altName, preferredName)
        }
      }

      persons map { case name =>
        replacedMap.getOrElse(name, name)
      }
    }

    replacedContextRdd flatMap {case (contextId, replacedNames) =>
      for(replacedName <- replacedNames) yield (contextId, replacedName)
    }
  }

  def metaphoneDedup = (rdd: RDD[(String, Map[String, Int])]) => {

    // Tokenize and DoubleMetaphone each name
    // Replace each part with possible DoubleMetaphone Nicknames
    // Output each possible alternative of the original name
    // AL GORE -> ALEXANDER GORE, ALBERT GORE, ALAN GORE, ALFRED GORE, etc.
    val alternativeMetaphoneRdd = rdd flatMap { case (name, others) =>
      getAlternativeHashes(name) map {alternative =>
        (alternative, (name, others.values.sum))
      }
    }

    // Group names sharing a same DoubleMetaphone
    // Find the preferred name (most frequent)
    // Yield a Map of replacement
    val replaceRdd = alternativeMetaphoneRdd.groupByKey().filter(_._2.size > 1) flatMap { case (candidate, it) =>
      val alternatives = it.toList.sortBy(_._2).reverse
      val bestAlternative = alternatives.head._1
      val others = alternatives.tail.map(_._1).map(metaphoneHash)
      for (other <- others) yield (other, bestAlternative)
    }

    // Transform the initial names into DoubleMetaphone
    // Join with our map of replacement and replace if needed
    // Update our hash map of alternative name frequency
    rdd map { case (name, others) =>
      (metaphoneHash(name), (name, others))
    } leftOuterJoin replaceRdd map { case (metaphone, ((name, others), replaceOpt)) =>
      (replaceOpt.getOrElse(name), others)
    } reduceByKey(_ |+| _)

  }

  private def getAlternativeHashes(name: String): List[String] = {
    val parts = metaphoneHash(name).split("#")
    findNicknames(parts)
  }

  private def metaphoneHash(name: String): String = {
    val dm = new DoubleMetaphone()
    val parts = name.split("\\s")
    parts.map({case part =>
      val hash = dm.doubleMetaphone(part)
      if(hash == null) {
        ""
      } else {
        hash
      }
    }).sorted.mkString("#")
  }

  private def findNicknames(parts: Array[String]): List[String] = {
    val candidates = collection.mutable.MutableList[String]()
    for (i <- parts.indices) {
      val left = parts.take(i).mkString(" ").trim
      val right = parts.takeRight(parts.length - i - 1).mkString(" ").trim
      val trueNames = allNicknames.getOrElse(parts(i), Set(parts(i)))
      for (trueName <- trueNames) {
        val newName = s"$left $trueName $right".trim
        candidates += newName
      }
    }

    candidates.distinct.toList
  }

}

object NameDeduplication {

  val stopWords = Set("mr", "mrs", "miss", "jr", "sr", "phd", "sir", "dr")

  case class NickNameMetaphone(
                                pNTxt: String,
                                mPNTxt: String,
                                sNTxt: String,
                                mSNTxt: String
                              )

  def loadNicknames(url: URL) = {
    val nicknames = scala.io.Source.fromURL(url).getLines()
    nicknames.map({line =>
      val a = line.split(",")
      NickNameMetaphone(a(0).trim, a(1).trim, a(2).trim, a(3).trim)
    }).toList.groupBy(_.mSNTxt).mapValues(_.map(_.mPNTxt).toSet)
  }

}
