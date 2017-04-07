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

import java.text.SimpleDateFormat

import com.gravity.goose.{Configuration, Goose}
import io.gzet.HtmlFetcher.Content
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD

class HtmlFetcher(
                   connectionTimeout: Int = 10000,
                   socketTimeout: Int = 10000
                 ) extends Serializable {

  final val USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36"
  final val ISO_SDF = "yyyy-MM-dd'T'HH:mm:ssZ"

  def fetchWithContext(urlRdd: RDD[(Long, String)]): RDD[(Long, Content)] = {
    urlRdd mapPartitions { urls =>
      val sdf = new SimpleDateFormat(ISO_SDF)
      val goose = getGooseScraper
      urls map { case (id, url) =>
        (id, fetchUrl(goose, url, sdf))
      }
    }
  }

  def fetch(urlRdd: RDD[String]): RDD[Content] = {
    urlRdd mapPartitions { urls =>
      val sdf = new SimpleDateFormat(ISO_SDF)
      val goose = getGooseScraper
      urls map(url => fetchUrl(goose, url, sdf))
    }
  }

  private def getGooseScraper: Goose = {
    val conf: Configuration = new Configuration
    conf.setEnableImageFetching(false)
    conf.setBrowserUserAgent(USER_AGENT)
    conf.setConnectionTimeout(connectionTimeout)
    conf.setSocketTimeout(socketTimeout)
    new Goose(conf)
  }

  private def fetchUrl(goose: Goose, url: String, sdf: SimpleDateFormat) : Content = {

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

      Content(url, title, description, body, publishDate)

    } catch {
      case e: Throwable =>
        Content(url, None, None, None, None)
    }
  }
}


object HtmlFetcher {

  case class Content(
                      url: String,
                      title: Option[String],
                      description: Option[String],
                      body: Option[String],
                      publishedDate: Option[String]
                    )

}
