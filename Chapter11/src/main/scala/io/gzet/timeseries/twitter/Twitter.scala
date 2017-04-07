package io.gzet.timeseries.twitter

import java.text.SimpleDateFormat
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap
import emoji4j.EmojiUtils
import io.gzet.timeseries.SimpleConfig
import io.gzet.timeseries.timely.Metric
import io.gzet.utils.spark.gis.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Try

case class Tweet(
                  date: Long,
                  body: String,
                  sentiment: Float,
                  state: Option[String],
                  geoHash: Option[String],
                  emojis: Array[String]
                ) {

  override def toString = {
    s"Tweet($date,$body,$sentiment,${state.getOrElse("")},${geoHash.getOrElse("")}),[${emojis.mkString(",")}]"
  }

}

object Twitter extends SimpleConfig {

  val states = Source.fromInputStream(this.getClass.getResourceAsStream("/states")).getLines().map({ s =>
    val a = s.toUpperCase().split("\\s", 2)
    (a.head, a.last)
  }).toMap

  private def sentiment(coreMap: CoreMap) = {
    coreMap.get(classOf[SentimentCoreAnnotations.ClassName]) match {
      case "Very negative" => 0
      case "Negative" => 1
      case "Neutral" => 2
      case "Positive" => 3
      case "Very positive" => 4
      case _ => throw new IllegalArgumentException(s"Could not get sentiment for [${coreMap.toString}]")
    }
  }

  def getSentimentAnnotator: StanfordCoreNLP = {
    val pipelineProps = new Properties()
    pipelineProps.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
    pipelineProps.setProperty("enforceRequirements", "false")
    new StanfordCoreNLP(pipelineProps)
  }


  def getLemmaAnnotator: StanfordCoreNLP = {
    val pipelineProps = new Properties()
    pipelineProps.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(pipelineProps)
  }

  def getAnnotator: StanfordCoreNLP = {
    val pipelineProps = new Properties()
    pipelineProps.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    new StanfordCoreNLP(pipelineProps)
  }

  def extractSentiment(text: String, annotator: StanfordCoreNLP = getSentimentAnnotator) = {
    val annotation = annotator.process(text)
    val sentences = annotation.get(classOf[SentencesAnnotation])
    val totalScore = sentences map sentiment
    if (sentences.nonEmpty) totalScore.sum.toFloat / sentences.size() else 2.0f
  }

  def stem(text: String, annotator: StanfordCoreNLP = getLemmaAnnotator) = {
    val annotation = annotator.process(text.clean)
    val sentences = annotation.get(classOf[SentencesAnnotation])
    sentences.flatMap({
      sentence =>
        sentence.get(classOf[TokensAnnotation]).map({
          token =>
            token.get(classOf[LemmaAnnotation])
        })
    }).mkString(" ")
  }

  def stemAndAnalyze(text: String, annotator: StanfordCoreNLP = getAnnotator) = {
    val annotation = annotator.process(text)
    val sentences = annotation.get(classOf[SentencesAnnotation])
    val totalScore = sentences map sentiment
    val avgSentiment = if (sentences.nonEmpty) totalScore.sum.toFloat / sentences.size() else 2.0f
    val cleaned = sentences.flatMap({
      sentence =>
        sentence.get(classOf[TokensAnnotation]).map({
          token =>
            token.get(classOf[LemmaAnnotation])
        })
    }).mkString(" ")
    (cleaned, avgSentiment)
  }

  val stemTweets = (it: Iterator[String]) => {

    val annotator = getLemmaAnnotator
    it map {
      tweet =>
        stem(tweet, annotator)
    }
  }

  val analyzeTweet = (it: Iterator[Status]) => {

    val annotator = getAnnotator

    it map { status =>

      val date = status.getCreatedAt.getTime
      val body = status.getText.clean

      val geoHash = Try {
        val lat = status.getGeoLocation.getLatitude
        val lng = status.getGeoLocation.getLongitude
        GeoHash.encode(lat, lng)
      }

      val state = Try {
        status.getUser.getLocation.split("\\s").map(_.toUpperCase()).filter({s =>
          states.contains(s)
        }).head
      }

      val cleaned = body.clean
      val (stemmed, sentiment) = stemAndAnalyze(cleaned, annotator)

      Tweet(
        date,
        stemmed.replaceAll("[^a-z#]", " ").split("\\s+").mkString(" "),
        sentiment,
        state.toOption,
        geoHash.toOption,
        body.emojis
      )
    }
  }

  val analyzeJson = (it: Iterator[String]) => {

    implicit val format = DefaultFormats
    val annotator = getAnnotator

    it map { tweet =>

      val json = parse(tweet)

      val date = Try(new SimpleDateFormat("MMM d, yyyy hh:mm:ss a").parse((json \ "createdAt").extract[String]).getTime).getOrElse(0L)
      val text = Try((json \ "text").extract[String]).getOrElse("")
      val location = Try((json \ "user" \ "location").extract[String]).getOrElse("").toLowerCase()

      val state = Try {
        location.split("\\s").map(_.toUpperCase()).filter({s =>
          states.contains(s)
        }).head
      }

      val geoHash = Try {
        val lat = (json \ "geoLocation" \ "latitude").extract[Double]
        val lng = (json \ "geoLocation" \ "longitude").extract[Double]
        GeoHash.encode(lat, lng)
      }

      val cleaned = text.clean
      val (stemmed, sentiment) = stemAndAnalyze(cleaned, annotator)

      Tweet(
        date,
        stemmed.replaceAll("[^a-z#]", " ").split("\\s+").mkString(" "),
        sentiment,
        state.toOption,
        geoHash.toOption,
        text.emojis
      )
    }
  }

  def getTwitterStream(ssc: StreamingContext, filters: Seq[String] = Nil) = {
    val builder = new ConfigurationBuilder()
    builder.setOAuthConsumerKey(twitterApiKey)
    builder.setOAuthConsumerSecret(twitterApiSecret)
    builder.setOAuthAccessToken(twitterTokenKey)
    builder.setOAuthAccessTokenSecret(twitterTokenSecret)
    val configuration = builder.build()
    TwitterUtils.createStream(
      ssc,
      Some(new OAuthAuthorization(configuration)),
      filters,
      StorageLevel.MEMORY_ONLY
    )
  }

  implicit class TwitterCleanup(tweet: String) {

    val emojiR = "(:\\w+:)".r
    val hashtagR = "(#\\w+)".r

    def clean = {

      var text = tweet.toLowerCase()
      text = text.replaceAll("https?:\\/\\/\\S+", "")
      text = StringUtils.stripAccents(text)
      text = EmojiUtils.removeAllEmojis(text)

      text
        .trim
        .toLowerCase()
        .replaceAll("rt\\s+", "")
        .replaceAll("@[\\w\\d-_]+", "")
        .replaceAll("[^\\w#\\[\\]:'\\.!\\?,]+", " ")
        .replaceAll("\\s+([:'\\.!\\?,])\\1", "$1")
        .replaceAll("[\\s\\t]+", " ")
        .replaceAll("[\\r\\n]+", ". ")
        .replaceAll("(\\w)\\1{2,}", "$1$1") // avoid looooool
        .replaceAll("^\\W+", "") // do not start tweet with punctuation
        .replaceAll("\\s?([:'\\.!\\?,])\\1{0,}", "$1") // no lagging space on punctuation
        .replaceAll("#\\W", "")
        .replaceAll("[#':,;\\.]$", "")
        .trim
    }

    def emojis = {
      var text = tweet.toLowerCase()
      text = text.replaceAll("https?:\\/\\/\\S+", "")
      emojiR.findAllMatchIn(EmojiUtils.shortCodify(text)).map(_.group(1)).filter({
        emoji =>
          EmojiUtils.isEmoji(emoji)
      }).map(_.replaceAll("\\W", "")).toArray
    }
  }

  implicit class MetricBuilder(rdd: RDD[Tweet]) {

    def expandedTweets = rdd flatMap {
      tweet =>
        twitterFilter filter {
          f =>
            tweet.body.contains(f)
        } map {
          case "hillary" => ("clinton", tweet)
          case "clinton" => ("clinton", tweet)
          case "donald" => ("trump", tweet)
          case "trump" => ("trump", tweet)
        }
    }

    def countByState = {
      expandedTweets.map({
        case (tag, tweet) =>
          ((tag, tweet.date, tweet.state), 1)
      }).reduceByKey(_+_).map({
        case ((tag, date, state), count) =>
          Metric(
            s"io.gzet.count.state.$tag",
            date,
            count,
            Map("state" -> state).filter(_._2.isDefined).map({
              case (k, v) =>
                (k, v.get)
            })
          )
      })
    }

    def sentimentByState = {
      expandedTweets.map({
        case (tag, tweet) =>
          ((tag, tweet.date, tweet.state), tweet.sentiment)
      }).groupByKey().mapValues({ f =>
        f.sum / f.size
      }).map({
        case ((tag, date, state), sentiment) =>
          Metric(
            s"io.gzet.stmt.state.$tag",
            date,
            sentiment,
            Map("state" -> state).filter(_._2.isDefined).map({
              case (k, v) =>
                (k, v.get)
            }),
            buildViz(sentiment)
          )
      })
    }
  }

  def buildViz(tone: Float) = {
    if(tone > 0 && tone <= 1.5) {
      Some("SECRET")
    } else if (tone > 1.5 && tone <= 2.5) {
      Some("INTERNAL")
    } else {
      Some("CONFIDENTIAL")
    }
  }


}
