//package io.gzet.story.clustering
//
//import io.gzet.story.html.{Tokenizer, WebScraping}
//import io.gzet.story.simhash.SimhashUtils._
//import io.gzet.story.utils.{SimpleConfig, Stopwords}
//import org.apache.spark.mllib.clustering.{EMLDAOptimizer, LDA}
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.{SparkConf, SparkContext}
//
//object LDAClustering extends SimpleConfig {
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
//      val body = Tokenizer.lucene(content.body).filter(s => !stopwords.value.contains(s))
//      (body.mkString(" ").simhash, body)
//    }).filter(_._2.size > minWords)
//
//    val vocab = corpusRDD.sparkContext.broadcast(corpusRDD.flatMap(_._2).map(_ -> 1).reduceByKey(_ + _).sortBy(_._2).zipWithIndex.map(_._1).collectAsMap())
//    val vocabArray = vocab.value.toArray.sortBy(_._2).map(_._1)
//
//    val hashVectorRDD = corpusRDD.flatMap({ case (simhash, corpus) =>
//      corpus.map({ word =>
//        ((simhash.toLong, vocab.value.get(word).get), 1)
//      })
//    }).reduceByKey(_ + _).map({ case ((simhash, wordId), tf) =>
//      (simhash, (wordId, tf.toDouble))
//    }).groupByKey().mapValues({ wordCount =>
//      Vectors.sparse(vocab.value.size, wordCount.toSeq)
//    }).cache()
//
//    // Run lda.
//    val lda = new LDA()
//    val optimizer = new EMLDAOptimizer()
//    lda.setOptimizer(optimizer).setK(ldaTopics).setMaxIterations(ldaMaxIterations)
//    val ldaModel = lda.run(hashVectorRDD)
//
//    // Print the topics, showing the top-weighted terms for each topic.
//    val topicIndices = ldaModel.describeTopics(ldaMaxTermsPerTopic)
//    val topics = topicIndices.map { case (terms, termWeights) =>
//      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
//    }
//
//    println(s"Topics:")
//    topics.zipWithIndex.sortBy(_._2).foreach { case (topic, i) =>
//      println(s"TOPIC $i")
//      topic.foreach { case (term, weight) =>
//        println(s"$term\t$weight")
//      }
//      println()
//    }
//
//  }
//
//}
