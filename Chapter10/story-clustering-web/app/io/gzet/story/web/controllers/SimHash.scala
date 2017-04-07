package io.gzet.story.web.controllers

import io.gzet.story.model.{Article, Duplicate}
import io.gzet.story.util.{HtmlFetcher, Tokenizer}
import io.gzet.story.util.SimhashUtils._
import io.gzet.story.web.SimpleConfig
import io.gzet.story.web.dao.CassandraDao
import org.apache.lucene.analysis.en.EnglishAnalyzer
import play.api.Logger
import play.api.libs.functional.syntax._
import play.api.libs.json._
import play.api.mvc.{Action, Controller}

object SimHash extends Controller with SimpleConfig {

  val dao = new CassandraDao
  val goose = new HtmlFetcher(gooseConnectionTimeout, gooseSocketTimeout)
  val analyzer = new EnglishAnalyzer()

  implicit val articleFormat = {
    val jsonDescription =
      (__ \ "hash").format[Int] and (__ \ "body").format[String] and (__ \ "title").format[String] and (__ \ "url").format[String]

    jsonDescription(Article.apply, unlift(Article.unapply))
  }

  implicit val duplicateFormat = {
    val jsonDescription =
      (__ \ "simhash").format[Int] and (__ \ "body").format[String] and (__ \ "title").format[String] and (__ \ "url").format[String] and (__ \ "related").format[List[Article]]

    jsonDescription(Duplicate.apply, unlift(Duplicate.unapply))
  }

  def fetch = Action { implicit request =>

    val url = request.getQueryString("url").getOrElse("NA")
    Logger.info(s"Fetch article [$url]")

    val article = goose.fetch(url)
    Logger.info(s"Title [${article.title}]")

    val hash = Tokenizer.lucene(article.body, analyzer).mkString(" ").simhash
    Logger.info(s"Hash [$hash]")

    val related = dao.findDuplicates(hash)
    Logger.info(s"${related.size} duplicate(s) found")

    Ok(Json.toJson(Duplicate(
      hash,
      article.body,
      article.title,
      url,
      related
    )))

  }


}
