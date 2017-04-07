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

import com.datastax.driver.core.Cluster
import io.gzet.recommender.Node

import scala.collection.JavaConversions._

class CassandraDao(cassandraHost: String, cassandraPort: Int) {

  private val cluster = Cluster.builder().addContactPoint(cassandraHost).withPort(cassandraPort).build()
  val session = cluster.connect()

  private def exists(tableName: String): Boolean = {
    cluster.getMetadata.getKeyspace("music").getTable(tableName) != null
  }

  def dropSongs = {
    session.execute(s"DROP table IF EXISTS music.hash;")
    session.execute(s"DROP table IF EXISTS music.record;")
  }

  def dropPlaylist = {
    session.execute(s"DROP table IF EXISTS music.nodes;")
    session.execute(s"DROP table IF EXISTS music.edges;")
  }

  def findSongsByHash(hash: String): List[Long] = {
    if(!exists("hash")) return List[Long]()
    val stmt = s"SELECT songs FROM music.hash WHERE id = '$hash';"
    val results = session.execute(stmt)
    results flatMap { row =>
      row.getList("songs", classOf[java.lang.Long]).map(_.toLong)
    } toList
  }

  def getSongs: List[String] = {
    if(!exists("record")) return List[String]()
    val stmt = s"SELECT name FROM music.record;"
    val results = session.execute(stmt)
    results map { row =>
      row.getString("name")
    } toList
  }

  def getSongName(songId: Long): Option[String] = {
    if(!exists("record")) return None
    val stmt = s"SELECT name FROM music.record WHERE id = $songId;"
    val results = session.execute(stmt)
    val songNames = results map { row =>
      row.getString("name")
    }

    if(songNames.isEmpty) None: Option[String] else Some(songNames.head)
  }

  def getSongId(songName: String): Option[Long] = {
    if(!exists("record")) return None
    val stmt = s"SELECT id FROM music.record WHERE name = '$songName';"
    val results = session.execute(stmt)
    val songIds = results map { row =>
      row.getLong("id")
    }

    if(songIds.isEmpty) None: Option[Long] else Some(songIds.head)
  }

  def getNodes: List[Node] = {
    if(!exists("nodes")) return List[Node]()
    val stmt = s"SELECT id, name, popularity FROM music.nodes;"
    val results = session.execute(stmt)
    results map { row =>
      val id = row.getLong("id")
      val name = row.getString("name")
      val popularity = row.getDouble("popularity")
      Node(id, name, popularity)
    } toList
  }

  def close() {
    session.close()
    cluster.close()
  }

}