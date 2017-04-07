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

package svc

import com.typesafe.config.ConfigFactory
import io.gzet.recommender.Audio

class AnalyzerSvc() {

  val config = ConfigFactory.load()
  val cassandraHost = config.getString("cassandra.host")
  val cassandraPort = config.getInt("cassandra.port")
  val sampleSize = config.getDouble("gzet.sample.size")
  val minMatch = config.getDouble("gzet.min.match")
  val dao = new CassandraDao(cassandraHost, cassandraPort)

  def analyze(audio: Audio): Option[String] = {

    val samples = audio.sampleByTime(sampleSize)
    val hashes = samples.map(_.hash)
    val hashResults = hashes.map(dao.findSongsByHash).reduce(_ ++ _)

    if(hashResults.isEmpty) {
      None: Option[String]
    } else {
      val songIds = hashResults.groupBy(s => s).mapValues(_.length).toList.sortBy(_._2).reverse
      val (bestId, bestMatch) = songIds.head
      val score = bestMatch / hashes.size.toDouble
      if(score > minMatch) {
        dao.getSongName(bestId)
      } else {
        None: Option[String]
      }
    }
  }
}
