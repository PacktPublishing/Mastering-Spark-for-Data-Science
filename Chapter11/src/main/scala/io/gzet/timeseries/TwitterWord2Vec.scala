package io.gzet.timeseries

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TwitterWord2Vec {

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("Anomaly Detection")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    import io.gzet.timeseries.twitter.Twitter._
    import org.json4s.DefaultFormats
    import org.json4s.jackson.JsonMethods._
    import scala.util.Try

    sc.textFile("file:///Users/antoine/CHAPTER/twitter-all").filter({
      s =>
        implicit val format = DefaultFormats
        val json = parse(s)
        val lang = Try((json \ "lang").extract[String]).getOrElse("na")
        val text = Try((json \ "text").extract[String]).getOrElse("")
        lang == "en" && text.split("\\W+").length > 5
    }) mapPartitions analyzeJson saveAsObjectFile "file:///Users/antoine/CHAPTER/twitter-all-sentiments"

    //TODO: ***********************
    //TODO: EXTRACT TWEETS FEATURES
    //TODO: ***********************

    import io.gzet.timeseries.twitter.Tweet
    val input = "file:///Users/antoine/CHAPTER/twitter-all-sentiments"
    val tweetRDD = sc.objectFile[Tweet](input, 4).cache
    val tweetDF = tweetRDD.toDF

    //TODO: *************
    //TODO: LEARN CONTEXT
    //TODO: *************

    import org.apache.spark.mllib.feature.Word2Vec
    val corpusRDD = tweetRDD.map(_.body.split("\\s").toSeq).filter(_.toSet.size > 4)
    val model = new Word2Vec().fit(corpusRDD)
    val bModel = sc.broadcast(model)

    //TODO: *********************
    //TODO: Playing with word2vec
    //TODO: *********************


    import org.apache.spark.mllib.linalg.Vectors
    def association(word1: String, word2: String, word3: String) = {
      val isTo = model.getVectors.get(word1).get.zip(model.getVectors.get(word3).get).map(t => t._1 - t._2)
      val what = model.getVectors.get(word2).get
      val vec = isTo.zip(what).map(t => t._1 + t._2).map(_.toDouble)
      Vectors.dense(vec)
    }

    model.findSynonyms("mexican", 10).foreach(println)
    /*
    (depoall,1.5145559063695229)
    (immigra,1.5089770842972865)
    (depothe,1.4998101448944554)
    (refugee,1.4562307539693566)
    (muslim,1.4550708079632857)
    (immigrant,1.4440790680876634)
    (jew,1.3359889525438184)
    (arab,1.2969842137356713)
    (lgb,1.2963428750976882)
    (peso,1.2961368533583244)
     */

    model.findSynonyms(association("trump", "cunt", "hillary"), 10).foreach(println)
    /*
    (twat,1.376903883375691)
    (fucktard,1.3424233432792023)
    (homphobe,1.3268033164001134)
    (prick,1.3235856231612548)
    (asshole,1.276210428142504)
    (moldy,1.2742937410015667)
    (homo,1.2702018128760928)
    (pig,1.251723414517948)
    (umpa,1.2450007843315989)
    (moron,1.2306696405230175)
     */

    val word10K = tweetRDD.flatMap(_.body.split("\\s")).map(_ -> 1).reduceByKey(_ + _).sortBy(_._2, ascending = false).keys.take(10000).toSet
    val words = model.getVectors.filter(t => word10K.contains(t._1)).toSeq

    printToFile(new java.io.File("/Users/antoine/CHAPTER/word2vec-vectors")) { p =>
      words.map(_._2).map(_.mkString("\t")) foreach p.println
    }

    printToFile(new java.io.File("/Users/antoine/CHAPTER/word2vec-meta")) { p =>
      words.map(_._1) foreach p.println
    }

    //TODO: ***********************
    //TODO: Building tweet features
    //TODO: ***********************

    val electionHashTags = sc.broadcast((model.findSynonyms("#maga", 100).map(_._1).toSet ++ model.findSynonyms("#strongertogether", 100).map(_._1).toSet).filter(_.startsWith("#")))
    val top = tweetRDD.flatMap(t => t.body.split("\\s").filter(_.startsWith("#"))).filter(electionHashTags.value.contains).map(_ -> 1).reduceByKey(_ + _).sortBy(_._2, ascending = false)
    val topHashTags = sc.broadcast(top.keys.collect.toSet)
    top.collect.foreach(println)

    def cosineSimilarity(x: Array[Float], y: Array[Float]): Double = {
      val dot = x.zip(y).map(a => a._1 * a._2).sum
      val magX = math.sqrt(x.map(i => i * i).sum)
      val magY = math.sqrt(y.map(i => i * i).sum)
      dot / (magX * magY)
    }

    val trump = model.getVectors.get("#maga").get
    val clinton = model.getVectors.get("#imwithher").get
    val love = model.getVectors.get("love").get
    val hate = model.getVectors.get("hate").get

    case class Word2Score(trump: Double, clinton: Double, love: Double, hate: Double)
    val word2Score = sc.broadcast(model.getVectors.map({ case (word, vector) =>
      val scores = Word2Score(
        cosineSimilarity(vector, trump),
        cosineSimilarity(vector, clinton),
        cosineSimilarity(vector, love),
        cosineSimilarity(vector, hate)
      )
      (word, scores)
    }))

    import org.apache.spark.sql.functions._

    val lov = sc.broadcast(Set("heart", "heart_eyes", "kissing_heart", "hearts", "kiss"))
    val joy = sc.broadcast(Set("joy", "grin", "laughing", "grinning", "smiley", "clap", "sparkles"))
    val jok = sc.broadcast(Set("wink", "stuck_out_tongue_winking_eye", "stuck_out_tongue"))
    val sad = sc.broadcast(Set("weary", "tired_face", "unamused", "frowning", "grimacing", "disappointed"))
    val cry = sc.broadcast(Set("sob", "rage", "cry", "scream", "fearful", "broken_heart"))
    val allEmojis = sc.broadcast(lov.value ++ joy.value ++ jok.value ++ sad.value ++ cry.value)

    val filterEmojis = udf((emojis: collection.mutable.WrappedArray[String]) => {
      emojis.exists(allEmojis.value.contains)
    })

    val filterText = udf((body: String) => {
      val words = body.split("\\s")
      val existScore = words.exists(word2Score.value.contains)
      val existTag = words.filter(_.startsWith("#")).exists(topHashTags.value.contains)
      existScore && existTag
    })

    val featureEmojis = udf((emojis: collection.mutable.WrappedArray[String]) => {
      val valid = emojis.filter(allEmojis.value.contains)
      valid.map({ emoji =>
        if (lov.value.contains(emoji)) {
          1.0
        } else if (joy.value.contains(emoji)) {
          0.7
        } else if (jok.value.contains(emoji)) {
          0.6
        } else if (sad.value.contains(emoji)) {
          0.3
        } else if (cry.value.contains(emoji)) {
          0.0
        } else 0.5
      }).sum / valid.length
    })

    val extractHashtags = udf((body: String) => {
      body.split("\\s").filter(_.startsWith("#")).filter(word2Score.value.contains)
    })

    val extractWords = udf((body: String) => {
      body.split("\\s").filter(word2Score.value.contains)
    })

    val featureTrump = udf((words: collection.mutable.WrappedArray[String]) => {
      words.map(word2Score.value.get).map(_.get.trump).sum / words.length
    })

    val featureClinton = udf((words: collection.mutable.WrappedArray[String]) => {
      words.map(word2Score.value.get).map(_.get.clinton).sum / words.length
    })

    val featureLove = udf((words: collection.mutable.WrappedArray[String]) => {
      words.map(word2Score.value.get).map(_.get.love).sum / words.length
    })

    val featureHate = udf((words: collection.mutable.WrappedArray[String]) => {
      words.map(word2Score.value.get).map(_.get.hate).sum / words.length
    })

    val buildVector = udf((sentiment: Double, tone: Double, trump: Double, clinton: Double, love: Double, hate: Double) => {
      Vectors.dense(Array(sentiment, tone, trump, clinton, love, hate))
    })

    val featureTweetDF = tweetDF
      .drop("state")
      .drop("geoHash")
      .drop("date")
      .filter(filterText($"body"))
      .filter(filterEmojis($"emojis"))
      .withColumn("hashtags", extractHashtags($"body"))
      .withColumn("words", extractWords($"body"))
      .withColumn("tone", featureEmojis($"emojis"))
      .withColumn("trump", featureTrump($"hashtags"))
      .withColumn("clinton", featureClinton($"hashtags"))
      .withColumn("love", featureLove($"words"))
      .withColumn("hate", featureHate($"words"))
      .withColumn("features", buildVector($"sentiment", $"tone", $"trump", $"clinton", $"love", $"hate"))

    import org.apache.spark.ml.feature.Normalizer
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("vector")
      .setP(1.0)

    val vectorTweetDF = normalizer.transform(featureTweetDF).cache()
    vectorTweetDF.show()

    //TODO: ****************
    //TODO: Detect anomalies
    //TODO: ****************

    import org.apache.spark.ml.clustering.KMeans

    new KMeans()
      .setFeaturesCol("vector")
      .setPredictionCol("cluster")
      .setK(5)
      .setMaxIter(Int.MaxValue)
      .setInitMode("k-means||")
      .setInitSteps(10)
      .setTol(0.01)
      .fit(vectorTweetDF)
      .write
      .save("file:///Users/antoine/CHAPTER/twitter-kmeans")

    import org.apache.spark.ml.clustering.KMeansModel
    val kmeansModel = KMeansModel.load("file:///Users/antoine/CHAPTER/twitter-kmeans")

    val centers = sc.broadcast(kmeansModel.clusterCenters)
    import org.apache.spark.mllib.linalg.Vector
    val euclidean = udf((v: Vector, cluster: Int) => {
      math.sqrt(centers.value(cluster).toArray.zip(v.toArray).map({
        case (x1, x2) => math.pow(x1 - x2, 2)
      }).sum)
    })

    kmeansModel.transform(vectorTweetDF)
      .withColumn("distance", euclidean($"vector", $"cluster"))
      .write
      .parquet("file:///Users/antoine/CHAPTER/twitter-clusters")

    val clusterTweetDF = sqlContext.read.parquet("file:///Users/antoine/CHAPTER/twitter-clusters")
    clusterTweetDF.registerTempTable("clustering")
    sqlContext.cacheTable("clustering")


    val vectorToString = udf((vector: Vector) => {
      vector.toArray.mkString("\t")
    })

    val tweetToString = udf((body: String, emojis: collection.mutable.WrappedArray[String], cluster: Int, distance: Double, vector: Vector) => {
      body + "\t" + emojis.mkString(" ") + "\t" + cluster + "\t" + distance + "\t" + vector.toArray.mkString("\t")
    })

    val clusterTweetStringRDD = clusterTweetDF
      .withColumn("metaString", tweetToString($"body", $"emojis", $"cluster", $"distance", $"vector"))
      .withColumn("vectorString", vectorToString($"vector"))
      .select("metaString", "vectorString").rdd.map(r => (r.getString(0), r.getString(1)))

    printToFile(new java.io.File("/Users/antoine/CHAPTER/twitter-vectors")) { p =>
      clusterTweetStringRDD.values.collect() foreach p.println
    }

    printToFile(new java.io.File("/Users/antoine/CHAPTER/twitter-meta")) { p =>
      clusterTweetStringRDD.keys.collect() foreach p.println
    }

    //TODO: *******************
    //TODO: Building Word2Graph
    //TODO: *******************

    val bDictionary = sc.broadcast(model.getVectors.keys.toList.zipWithIndex.map(l => (l._1, l._2.toLong + 1L)).toMap)

    import org.apache.spark.graphx._
    val wordRDD = sc.parallelize(model.getVectors.keys.toSeq.filter(s => s.length > 3))
    val word2EdgeRDD = wordRDD.mapPartitions({
      it =>
        val model = bModel.value
        val dictionary = bDictionary.value
        it.flatMap({
          from =>
            val synonyms = model.findSynonyms(from, 5)
            val tot = synonyms.map(_._2).sum
            synonyms.map({
              case (to, sim) =>
                val norm = sim / tot
                Edge(dictionary.get(from).get, dictionary.get(to).get, norm)
            })
        })
    })

    import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
    val word2Graph = Graph.fromEdges(word2EdgeRDD, 0L).partitionBy(RandomVertexCut, 100)
    word2Graph.cache()
    word2Graph.vertices.count()

    //TODO: **********************
    //TODO: Play with godwin point
    //TODO: **********************

    val components = word2Graph.connectedComponents().vertices.values.distinct().count
    println(s"Do we still have faith in humanity? ${components > 1L}")
    if(components == 1L) {
      println(s"Given enough time, any topic may lead to Hitler")
    }

    import io.gzet.timeseries.graph.Godwin
    val godwinPoint = bDictionary.value.get("hitler").get
    val godwinProcessor = new Godwin(Seq(godwinPoint))
    val distanceToGodwin = godwinProcessor.distance(word2Graph).mapValues(d => 1.0 - d)
    val godwinDistances = distanceToGodwin.collectAsMap()

    // Generate some random walks
    val seed = "love"
    val maxHops = 1000

    printToFile(new java.io.File(s"/Users/antoine/CHAPTER/walks/$seed")) { p =>
      godwinProcessor.randomWalks(word2Graph, bDictionary.value.get(seed).get, maxHops).sortBy(_._2).collect().map({ case (vid, hop) =>
        val word = bDictionary.value.map(_.swap).get(vid).get
        hop + "\t" + godwinDistances.getOrElse(vid, 0.0d) + "\t" + word
      }) foreach p.println
    }

  }

}


//
//  model.findSynonyms("#nevertrump", 10).foreach(println)
//  /*
//(#dumptrump,1.302518867085594)
//(#bernieorbust,1.2592152670048071)
//(#electionresults,1.2512766564296167)
//(#presidenthillaryclinton,1.242704925137895)
//(lockha,1.2388198370955688)
//(#imwithher,1.236643151812226)
//(#msmbias,1.2288999416516702)
//(#tytlive,1.2214804470927576)
//(#nastywomenunite,1.2177912376856417)
//(#imwither,1.2154617109418866)
//   */
//
//  model.findSynonyms("#lockherup", 10).foreach(println)
//  /*
//(#hillaryforprison,2.3266071900089313)
//(#neverhillary,2.2890002973310066)
//(#draintheswamp,2.2440446323298175)
//(#trumppencelandslide,2.2392471034643604)
//(#womenfortrump,2.2331140131326874)
//(#trumpwinsbecause,2.2182999853485454)
//(#imwithhim,2.1950198833564563)
//(#deplorable,2.1570936207197016)
//(#trumpsarmy,2.155859656266577)
//(#rednationrising,2.146132149205829)
//   */
//
//  model.findSynonyms("cunt", 10).foreach(println)
//  /*
//(prick,1.4217174285655207)
//(twat,1.366009457316916)
//(asshole,1.352977165240131)
//(moron,1.3055499479438102)
//(motherfucker,1.2675953110536298)
//(ass,1.2381804904342688)
//(homphobe,1.2079205476232433)
//(goofy,1.2063433592033646)
//(overused,1.1969070196491076)
//(bitch,1.1851455101663861)
//  */
//
//
//  model.findSynonyms("mexican", 10).foreach(println)
//  /*
//(muslim,2.061248051238901)
//(queer,2.0503021405625255)
//(refugee,1.9725814256623198)
//(racialized,1.839288086568739)
//(depoall,1.8082956110620565)
//(folx,1.8017065422678047)
//(pansexual,1.7862648473417133)
//(illegal,1.7613649638491156)
//(lgtbq,1.7552518176456258)
//(lgbtqa,1.7457606094322569)
//   */
//
