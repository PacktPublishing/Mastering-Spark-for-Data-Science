//package io.gzet.story.clustering
//
//import io.gzet.story.html.{Tokenizer, WebScraping}
//import io.gzet.story.simhash.SimhashUtils._
//import io.gzet.story.utils.{SimpleConfig, Stopwords}
//import org.apache.spark.mllib.clustering.PowerIterationClustering
//import org.apache.spark.{SparkConf, SparkContext}
//
//object SimhashPIClustering extends SimpleConfig {
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
//    val corpusRDD = contentRDD.zipWithIndex().map({ case (content, id) =>
//      (id, Tokenizer.lucene(content.body).filter(s => !stopwords.value.contains(s)))
//    }).filter(_._2.size > minWords)
//
//    val simhashRDD = corpusRDD.map({ case (id, text) =>
//      (id, text.mkString(" ").simhash)
//    })
//
//    val dedupRDD = simhashRDD.flatMap({ case (id, hash) =>
//      searchmasks.map(mask => (mask ^ hash, (id, hash)))
//    }).groupByKey().flatMap({ case (mask, ids) =>
//      ids.map({ case (id, hash) =>
//        (id, hash, ids)
//      })
//    }).flatMap({ case (id1, hash1, ids) =>
//      ids.map({ case (id2, hash2) =>
//        (id1, id2, hash1.similarity(hash2))
//      })
//    }).filter({ case (id1, id2, _) =>
//      id1 != id2
//    })
//
//    dedupRDD.cache()
//
//    val model = new PowerIterationClustering()
//      .setK(picClusters)
//      .setMaxIterations(picMaxIterations)
//      .setInitializationMode("degree")
//      .run(dedupRDD)
//
//    model.assignments.map({ a =>
//      (a.id, a.cluster)
//    }).join(corpusRDD).values.mapValues(_.take(50).mkString(" ")).sortBy(_._1).collect().foreach(println)
//
//    model.assignments.map({ a =>
//      (a.id, 1)
//    }).reduceByKey(_ + _).collect().foreach({ case (cluster, count) =>
//      println(s"cId: $cluster, count:$count")
//    })
//
//  }
//
//}
