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
import models.{Library, Songs}
import org.apache.commons.lang3.StringUtils
import play.api.Logger
import play.api.data.Form
import play.api.data.Forms._
import play.api.mvc._
import svc.{AnalyzerSvc, CassandraDao, SparkSvc}

object Index extends Controller {

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

  val indexForm: Form[Library] = Form(mapping("path" -> text)(Library.apply)(Library.unapply))

  def index = Action { implicit request =>
    val songs = Songs(dao.getSongs)
    Logger.info(s"Database is currently ${songs.songs.size} songs long")
    Ok(views.html.index(indexForm)(songs))
  }

  def submit = Action { implicit request =>
    indexForm.bindFromRequest.fold(
      errors =>
        Redirect(routes.Index.index()).flashing("error" -> s"Missing path"),
      index =>
        try {
          if(StringUtils.isNotEmpty(index.path)) {
            Logger.info("Dropping database")
            dao.dropSongs
            dao.dropPlaylist
            Logger.info("Submitting job")
            val jobId = spark.index(index.path)
            Redirect(routes.Index.index()).flashing("success" -> jobId)
          } else {
            Redirect(routes.Index.index()).flashing("error" -> s"Missing path")
          }
        } catch {
          case e: Exception =>
            Redirect(routes.Index.index()).flashing("error" -> e.getMessage)
        }
    )
  }
}