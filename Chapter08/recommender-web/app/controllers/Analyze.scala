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

import java.io.{File, FileInputStream}
import java.util.UUID

import com.typesafe.config.ConfigFactory
import io.gzet.recommender.Audio
import models.Songs
import play.api.Logger
import play.api.mvc._
import svc.{AnalyzerSvc, CassandraDao, SparkSvc}

object Analyze extends Controller {

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
    val songs = Songs(dao.getSongs)
    Logger.info(s"Database is currently ${songs.songs.size} songs long")
    Ok(views.html.analyze("Select a wav file to analyze")(songs))
  }

  def submit = Action(parse.multipartFormData) { request =>
    val songs = Songs(dao.getSongs)
    Logger.info(s"Database is currently ${songs.songs.size} songs long")
    if(songs.songs.isEmpty) {
      Redirect(routes.Analyze.index()).flashing("warning" -> s"Library is currently empty. Please index new records")
    } else {
      request.body.file("song").map { upload =>
        val fileName = upload.filename
        Logger.info(s"Processing file $fileName")
        val file = new File(s"/tmp/${UUID.randomUUID()}")
        upload.ref.moveTo(file)
        try {
          val song = process(file)
          if(song.isEmpty) {
            Redirect(routes.Analyze.index()).flashing("warning" -> s"Could not match any record for [$fileName]")
          } else {
            val songName = song.get
            Logger.info(s"Found song [$songName]")
            Redirect(routes.Analyze.index()).flashing("success" -> songName)
          }
        } catch {
          case e: Exception =>
            Redirect(routes.Analyze.index()).flashing("error" -> e.getMessage)
        }
      }.getOrElse {
        Redirect(routes.Analyze.index()).flashing("error" -> "Missing file")
      }
    }
  }

  def process(file: File) = {
    val is = new FileInputStream(file)
    val audio = Audio.processSong(is, minTime, maxTime)
    Logger.info(audio.toString)
    file.delete()
    analyzer.analyze(audio)
  }

}