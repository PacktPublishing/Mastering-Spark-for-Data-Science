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

package controllers

import com.typesafe.config.ConfigFactory
import play.api.Logger
import play.api.mvc.{Action, Controller}
import svc.{AnalyzerSvc, CassandraDao, SparkSvc}

object Playlist extends Controller {

  val config = ConfigFactory.load()
  val minTime = config.getInt("gzet.min.time")
  val maxTime = config.getInt("gzet.max.time")
  val cassandraHost = config.getString("cassandra.host")
  val cassandraPort = config.getInt("cassandra.port")
  val sampleSize = config.getDouble("gzet.sample.size")
  val minMatch = config.getDouble("gzet.min.match")

  val dao = new CassandraDao(cassandraHost, cassandraPort)
  val analyzer = new AnalyzerSvc()
  val spark = new SparkSvc()

  def index = Action { implicit request =>
    val playlist = models.Playlist(dao.getNodes)
    Logger.info(s"Database is currently ${playlist.nodes.size} songs long")
    Ok(views.html.playlist(playlist))
  }

  def personalize(id: Long) = Action { implicit request =>
    if(models.Playlist(dao.getNodes).nodes.isEmpty) {
      Redirect(routes.Playlist.index()).flashing("warning" -> s"Could not run personalized page rank on empty indices")
    } else {
      val name = dao.getSongName(id)
      if(name.isEmpty) {
        Redirect(routes.Playlist.index()).flashing("error" -> s"Could not find song for id [$id]")
      } else {
        try {
          Logger.info(s"Running a personalize Page Rank for id [$id] and song [$name]")
          val nodes = spark.playlist(id)
          val playlist = models.Playlist(nodes, name)
          Ok(views.html.playlist(playlist))
        } catch {
          case e: Exception =>
            Redirect(routes.Playlist.index()).flashing("error" -> e.getMessage)
        }
      }
    }
  }

  def submit = Action { implicit request =>
    try {
      dao.dropPlaylist
      val jobId = spark.playlist()
      Redirect(routes.Playlist.index()).flashing("success" -> jobId)
    } catch {
      case e: Exception =>
        Redirect(routes.Playlist.index()).flashing("error" -> e.getMessage)
    }
  }

}
