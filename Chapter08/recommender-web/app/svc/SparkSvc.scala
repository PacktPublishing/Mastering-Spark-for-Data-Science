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

import java.io.StringWriter
import java.net.{HttpURLConnection, URL}

import com.typesafe.config.ConfigFactory
import io.gzet.recommender.Node
import org.apache.commons.io.IOUtils
import play.api.Logger
import play.api.libs.json._

class SparkSvc() {

  val config = ConfigFactory.load()
  val host = config.getString("spark.job.server.host")
  val port = config.getInt("spark.job.server.port")
  val timeout = config.getInt("spark.job.server.timeout")
  val appName = config.getString("spark.job.server.app")
  val context = config.getString("spark.job.server.context")
  val indexJob = config.getString("spark.job.index")
  val playlistJob = config.getString("spark.job.playlist")
  val playlistRecommendJob = config.getString("spark.job.personalized.playlist")

  private def getConnection(endpoint: String, params: Option[String]) = {
    try {
      val url = new URL(endpoint)
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setDoOutput(true)
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Accept", "application/json")
      if(params.isDefined){
        val os = connection.getOutputStream
        os.write(params.get.getBytes())
        os.flush()
        os.close()
      }
      val inputStream = connection.getInputStream
      val writer = new StringWriter()
      IOUtils.copy(inputStream, writer, "UTF-8")
      val ret = writer.toString
      Json.parse(ret)
    } catch {
      case e: Exception =>
        throw new Exception("Job Failed: " + e.getMessage)
    }
  }

  private def parseResponse(json: JsValue) : String = {
    val jobId = (json \ "result" \ "jobId").asOpt[String]
    if(jobId.isDefined){
      s"Job submitted [${jobId.get}]"
    } else {
      val message = (json \ "result" \ "message").asOpt[String]
      if(message.isDefined){
        throw new Exception(s"Job failed: ${message.get}")
      }
      throw new Exception("Could not find Spark job id")
    }
  }

  def index(path: String): String = {
    Logger.info("Submitting INDEX job")
    val url = s"http://$host:$port/jobs?appName=$appName&classPath=$indexJob&context=$context"
    val params = "input.dir=\"" + path + "\""
    val json = getConnection(url, Some(params))
    parseResponse(json)
  }

  def playlist() = {
    Logger.info("Submitting PLAYLIST job")
    val url = s"http://$host:$port/jobs?appName=$appName&classPath=$playlistJob&context=$context"
    val json = getConnection(url, None)
    parseResponse(json)
  }

  def playlist(id: Long) = {
    Logger.info("Submitting RECOMMEND job")
    val url = s"http://$host:$port/jobs?appName=$appName&classPath=$playlistRecommendJob&context=$context&sync=true&timeout=$timeout"
    val params = s"song.id=$id"
    val json: JsValue = getConnection(url, Some(params))
    val array = (json \ "result").as[Array[String]]
    array.map({line =>
      val Array(id, pr, song) = line.split(",").take(3)
      Node(id.toLong, song, pr.toDouble)
    }).toList
  }

}
