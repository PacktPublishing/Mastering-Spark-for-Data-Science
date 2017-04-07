//package io.gzet.story.clustering
//
//import io.gzet.story.canopy.Clustering
//import io.gzet.story.html.{Tokenizer, WebScraping}
//import io.gzet.story.simhash.SimhashUtils._
//import io.gzet.story.utils.{SimpleConfig, Stopwords}
//import org.apache.spark.{SparkConf, SparkContext}
//
//object SimhashCanopyClustering extends SimpleConfig {
//
//  def main(args: Array[String]) = {
//
//    val sc = new SparkContext(new SparkConf().setAppName("Test"))
//    val stopwords = sc.broadcast(Stopwords.stopwords)
//
//    if (args.isEmpty)
//      throw new IllegalArgumentException("usage: <gdeltGKGInputDir>")
//
//    val gdeltInputDir = args.head
//    val contentRDD = WebScraping.fetchGKGArticles(sc.textFile(gdeltInputDir).repartition(partitions))
//    val corpusRDD = contentRDD.map({ content =>
//      Tokenizer.lucene(content.body).filter(s => !stopwords.value.contains(s))
//    }).filter(_.size > minWords)
//
//    val simhashRDD = corpusRDD.map({ text =>
//      (text.mkString(" ").simhash, text)
//    })
//
//    val model = new Clustering(canopyT1, canopyT2, canopyMinObservations).train(simhashRDD.keys)
//    simhashRDD.map(s => (model.predict(s._1), s._2)).sortBy({ case ((cId, dist), body) =>
//      (cId, dist)
//    }).map({ case ((cId, dist), body) =>
//      (cId, body.take(100).mkString(" "))
//    }).take(70).foreach(println)
//
//  }
//
//}
