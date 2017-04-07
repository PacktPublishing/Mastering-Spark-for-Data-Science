package io.gzet.story

import java.net.URL

import com.datastax.spark.connector._
import io.gzet.story.model.Article
import io.gzet.story.util.SimhashUtils._
import io.gzet.story.util.{HtmlFetcher, Tokenizer}
import io.gzet.utils.spark.gdelt.GKGParser
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkException}

import scala.util.Try

object SimhashIndexing extends SimpleConfig with Logging {

  def main(args: Array[String]) = {

    val sc = new SparkContext(new SparkConf().setAppName("GDELT Indexing"))

    if (args.isEmpty)
      throw new SparkException("usage: <gdeltInputDir>")

    val gdeltInputDir = args.head
    val gkgRDD = sc.textFile(gdeltInputDir)
      .map(GKGParser.toJsonGKGV2)
      .map(GKGParser.toCaseClass2)

    val urlRDD = gkgRDD.map(g => g.documentId.getOrElse("NA"))
      .filter(url => Try(new URL(url)).isSuccess)
      .distinct()
      .repartition(partitions)

    val contentRDD = urlRDD.mapPartitions({ it =>
      val html = new HtmlFetcher(gooseConnectionTimeout, gooseSocketTimeout)
      it map html.fetch
    })

    val corpusRDD = contentRDD.mapPartitions({ it =>
      val analyzer = new EnglishAnalyzer()
      it.map(content => (content, Tokenizer.lucene(content.body, analyzer)))
    }).filter({ case (content, corpus) =>
      corpus.length > minWords
    })

    //CREATE TABLE gzet.articles ( hash int PRIMARY KEY, url text, title text, body text );
    corpusRDD.mapValues(_.mkString(" ").simhash).map({ case (content, simhash) =>
      Article(simhash, content.body, content.title, content.url)
    }).saveToCassandra(cassandraKeyspace, cassandraTable)

  }

}
