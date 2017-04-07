package io.gzet.story.util

import com.gravity.goose.{Configuration, Goose}
import io.gzet.story.model.Content

class HtmlFetcher(connectionTimeout: Int, socketTimeout: Int) {

  final val USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36"

  val conf: Configuration = new Configuration
  conf.setEnableImageFetching(false)
  conf.setBrowserUserAgent(USER_AGENT)
  conf.setConnectionTimeout(connectionTimeout)
  conf.setSocketTimeout(socketTimeout)
  val goose = new Goose(conf)

  def fetch(url: String): Content = {
    try {
      val article = goose.extractContent(url)
      val body = article.cleanedArticleText
      val title = article.title
      Content(url, title, body)
    } catch {
      case e: Throwable =>
        Content(url, "", "")
    }
  }
}
