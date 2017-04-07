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

package io.gzet.tagging.html

import java.net.{HttpURLConnection, URL}
import java.text.SimpleDateFormat

import com.gravity.goose.{Configuration, Goose}
import io.gzet.tagging.html.HtmlHandler.Content
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD

class HtmlHandler(
                   connectionTimeout: Int = 10000,
                   socketTimeout: Int = 10000
                 ) extends Serializable {

  def fetch(urlRdd: RDD[String]): RDD[Option[Content]] = {
    urlRdd mapPartitions { urls =>
      val sdf = new SimpleDateFormat(HtmlHandler.ISO_SDF)
      val goose = getGooseScraper
      urls map (url => fetchUrl(goose, url, sdf))
    }
  }

  def fetchUrl(goose: Goose, url: String, sdf: SimpleDateFormat): Option[Content] = {

    try {

      val article = goose.extractContent(url)
      var body = None: Option[String]
      var title = None: Option[String]
      var description = None: Option[String]
      var publishDate = None: Option[String]

      if (StringUtils.isNotEmpty(article.cleanedArticleText))
        body = Some(article.cleanedArticleText)

      if (StringUtils.isNotEmpty(article.title))
        title = Some(article.title)

      if (StringUtils.isNotEmpty(article.metaDescription))
        description = Some(article.metaDescription)

      if (article.publishDate != null)
        publishDate = Some(sdf.format(article.publishDate))

      Some(Content(url, title, description, body, publishDate))

    } catch {
      case e: Throwable =>
        None: Option[Content]
    }
  }

  def parse(htmlRdd: RDD[String]): RDD[Option[Content]] = {
    htmlRdd mapPartitions { urls =>
      val sdf = new SimpleDateFormat(HtmlHandler.ISO_SDF)
      val goose = getGooseScraper
      urls map (url => parseHtml(goose, url, sdf))
    }
  }

  def parseHtml(html: String): Option[Content] = {
    val goose = getGooseScraper
    val sdf = new SimpleDateFormat(HtmlHandler.ISO_SDF)
    parseHtml(goose, html, sdf)
  }

  def getGooseScraper: Goose = {
    val conf: Configuration = new Configuration
    conf.setEnableImageFetching(false)
    conf.setBrowserUserAgent(HtmlHandler.USER_AGENT)
    conf.setConnectionTimeout(connectionTimeout)
    conf.setSocketTimeout(socketTimeout)
    new Goose(conf)
  }

  def parseHtml(goose: Goose, html: String, sdf: SimpleDateFormat): Option[Content] = {

    try {

      val article = goose.extractContent("http://www.foo.bar", html)
      var body = None: Option[String]
      var title = None: Option[String]
      var description = None: Option[String]
      var publishDate = None: Option[String]

      if (StringUtils.isNotEmpty(article.cleanedArticleText))
        body = Some(article.cleanedArticleText)

      if (StringUtils.isNotEmpty(article.title))
        title = Some(article.title)

      if (StringUtils.isNotEmpty(article.metaDescription))
        description = Some(article.metaDescription)

      if (article.publishDate != null)
        publishDate = Some(sdf.format(article.publishDate))

      Some(Content("", title, description, body, publishDate))

    } catch {
      case e: Throwable =>
        None: Option[Content]
    }
  }

  def fetchUrl(url: String): Option[Content] = {
    val goose = getGooseScraper
    val sdf = new SimpleDateFormat(HtmlHandler.ISO_SDF)
    fetchUrl(goose, url, sdf)
  }

}

object HtmlHandler {

  final val USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36"
  final val ISO_SDF = "yyyy-MM-dd'T'HH:mm:ssZ"

  def expandUrl(url: String): String = {
    var connection: HttpURLConnection = null
    try {
      connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
      connection.setInstanceFollowRedirects(false)
      connection.setUseCaches(false)
      connection.setRequestMethod("GET")
      connection.connect()
      val redirectedUrl = connection.getHeaderField("Location")
      if (StringUtils.isNotEmpty(redirectedUrl)) {
        redirectedUrl
      } else {
        url
      }
    } catch {
      case e: Throwable =>
        url
    } finally {
      if (connection != null)
        connection.disconnect()
    }
  }

  case class Content(
                      url: String,
                      title: Option[String],
                      description: Option[String],
                      body: Option[String],
                      publishedDate: Option[String]
                    )

}
