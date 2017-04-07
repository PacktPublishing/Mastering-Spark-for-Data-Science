//package io.gzet.story.clustering
//
//import io.gzet.story.html.{Tokenizer, WebScraping}
//import io.gzet.story.linalg.Embedding
//import io.gzet.story.utils.{SimpleConfig, Stopwords}
//import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
//import org.apache.spark.{SparkConf, SparkContext}
//
//object KMeansClustering extends SimpleConfig {
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
//    val tfModel = new HashingTF(1 << 20)
//    val tfRDD = tfModel transform corpusRDD
//
//    val tfIdfModel = new IDF().fit(tfRDD)
//    val tfIdfRDD = tfIdfModel transform tfRDD
//
//    val normalizer = new Normalizer()
//    val sparseVectorRDD = normalizer transform tfIdfRDD
//
//    val embedding = Embedding(Embedding.HIGH_DIMENSIONAL_RI)
//    val vectorRDD = sparseVectorRDD map embedding.embed
//    vectorRDD.cache()
//    vectorRDD.count()
//
//    val kmeansModel = new KMeans()
//      .setEpsilon(kMeansEpsilon)
//      .setK(kMeansClusters)
//      .setMaxIterations(kMeansMaxIterations)
//      .run(sparseVectorRDD)
//
//    kmeansModel.predict(sparseVectorRDD).zip(contentRDD).map({ case (cluster, content) =>
//      (cluster, content.title)
//    }).sortBy(_._1).collect().foreach(println)
//
//  }
//
//}
