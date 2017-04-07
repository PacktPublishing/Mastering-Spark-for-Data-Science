package io.gzet.story

import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import io.gzet.story.linalg.Embedding
import io.gzet.story.model.{Cluster, Content}
import io.gzet.story.util.SimhashUtils._
import io.gzet.story.util.{HtmlFetcher, Stopwords, Tokenizer}
import io.gzet.utils.spark.gdelt._
import io.gzet.utils.spark.gis.GeoHash
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.nifi.remote.client.SiteToSiteClient
import org.apache.nifi.spark.NiFiReceiver
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.{StreamingKMeans, StreamingKMeansModel}
import org.apache.spark.mllib.feature.{HashingTF, Normalizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import scala.util.Try

object StoryStreamClustering extends SimpleConfig with Logging {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Story Clustering")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Minutes(batchSize))
    val stopWords = sc.broadcast(Stopwords.stopwords)

    val minBatchId = getMinBatchId(sc)
    val minClusterId = getMinClusterId(sc)
    logInfo(s"Starting with batch: $minBatchId and cluster: $minClusterId")

    val batchIdAcc = sc.accumulator[Int](minBatchId, "batches")
    val globalClusterIdAcc = sc.accumulator[Int](minClusterId, "clusters")

    // Define a new Streaming KMeans model with random centers
    val model = new StreamingKMeans()
      .setK(kMeansClusters)
      .setRandomCenters(Embedding.mediumDimension, 0.0)
      .setHalfLife(kMeansHalfLife, "batches")

    // Read from Nifi
    val nifiStream = readFromNifi(ssc).repartition(partitions)

    // Parse GKG Events
    val gkgStream = parseGkg(nifiStream)

    // Extract distinct URLs
    val urlStream = extractDistinctUrls(gkgStream)

    // Fetch HTML Content
    val contentStream = fetchHtml(urlStream)
    contentStream.cache()
    contentStream.count().map(content => s"$content html pages").print()
    contentStream.map(_.title).print()

    // Tokenize HTML Content
    val stemmedContentStream = tokenizeContent(contentStream, stopWords)

    // Create TF vectors
    val contentVectorStream = buildTf(stemmedContentStream)

    // Train KMeans clustering
    model.trainOn(contentVectorStream.map(_._2))

    // Predict clusters
    val predictionStream = contentVectorStream.mapValues(v => (v, model.latestModel().predict(v)))

    predictionStream.map({ case (content, (v, cId)) =>
      (content.url, (content, v, cId))
    }).join(gkgStream.map({ gkg =>
      (gkg.documentId.getOrElse(UUID.randomUUID().toString), gkg)
    })).foreachRDD({ rdd =>

      val latestModel = model.latestModel()
      val batchId = batchIdAcc.value

      rdd.cache()
      val articlesCount = rdd.count()

      val localClusters = rdd.values.keys.map(_._3).distinct().collect()
      val globalClusterOffset = globalClusterIdAcc.value
      val globalClusterLocalMap = localClusters.map({ cId =>
        val newCId = globalClusterOffset + cId
        (cId, newCId)
      }).toMap

      // Increment counters
      batchIdAcc += 1
      globalClusterIdAcc += localClusters.length

      // *******************
      // Create new articles
      // *******************

      val articles = rdd.map({ case (_, ((content, v, cId), gkg)) =>
        Map(
          "uid" -> gkg.gkgId.getOrElse(UUID.randomUUID().toString),
          "cid" -> globalClusterLocalMap.get(cId).get,
          "gid" -> cId,
          "batch" -> batchId,
          "hash" -> content.body.simhash,
          "date" -> gkg.date.getOrElse(new Date().getTime),
          "url" -> content.url,
          "title" -> content.title,
          "body" -> content.body,
          "tone" -> Try(gkg.tones.get.averageTone.get).getOrElse(0.0d),
          "geo" -> extractGeohashes(gkg),
          "country" -> gkg.v2Locations.getOrElse(Array[Location]()).map(_.country.getOrElse("NA")).filter(c => c != "NA"),
          "theme" -> gkg.v2Themes.getOrElse(Array[Theme]()).map(_.theme),
          "person" -> gkg.v2Persons.getOrElse(Array[Person]()).map(_.person),
          "organization" -> gkg.v2Organizations.getOrElse(Array[Organization]()).map(_.organization),
          "vector" -> v.toArray.mkString(",")
        )
      })

      logInfo(s"[Batch-$batchId] - $articlesCount article(s) grouped within ${localClusters.length} cluster(s)")
      if (globalClusterLocalMap.nonEmpty) {

        // ***************************************
        // Gather cluster statistics / description
        // ***************************************

        val clusters = buildClusterStatistics(rdd, latestModel)
        val nodes = rdd.sparkContext.parallelize(clusters, 1).map({ cluster =>
          Map(
            "cid" -> globalClusterLocalMap.getOrElse(cluster.localId, cluster.localId),
            "gid" -> cluster.localId,
            "batch" -> batchId,
            "date" -> cluster.date,
            "articles" -> cluster.numArticles,
            "title" -> cluster.title,
            "url" -> cluster.url,
            "avgTone" -> cluster.avgTone,
            "theme" -> cluster.themes,
            "country" -> cluster.countries,
            "organization" -> cluster.organizations,
            "person" -> cluster.persons
          )
        })

        // ***************************************************************
        // Connect Stories by predicting old articles against new clusters
        // ***************************************************************

        val minBatchQuery = batchId - batchWindow
        val query = "{\"query\":{\"range\":{\"batch\":{\"gte\": " + minBatchQuery + ",\"lte\": " + batchId + "}}}}"
        val nodesDrift = rdd.sparkContext.esJsonRDD(esArticlesResource, query).map({ case (_, strJson) =>
          implicit val format = DefaultFormats
          val json = parse(strJson)
          val defaultVector = Array.fill[Double](Embedding.mediumDimension)(0.0d).mkString(",")
          val vector = Vectors.dense((json \ "vector").extractOrElse[String](defaultVector).split(",").map(_.toDouble))
          val previousCluster = (json \ "cid").extractOrElse[Int](-1)
          val newCluster = model.latestModel().predict(vector)
          ((previousCluster, globalClusterLocalMap.getOrElse(newCluster, newCluster)), 1)
        }).reduceByKey(_ + _)

        val edges = nodesDrift.map({ case ((previousCluster, newCluster), count) =>
          Map(
            "source" -> previousCluster,
            "target" -> newCluster,
            "weight" -> count
          )
        })

        logInfo(s"[Batch-$batchId] - Detected ${nodesDrift.filter(t => t._1._1 != t._1._2).count()} story drift(s)")
        clusters.sortBy(_.localId).foreach({ cluster =>
          logInfo(s"[Batch-$batchId] - [Cluster-${cluster.localId}] - ${cluster.numArticles} article(s)")
        })

        // Save new articles, nodes and edges to ES
        nodes.saveToEs(esNodesResource)
        edges.saveToEs(esEdgesResource)

      }

      articles.saveToEs(esArticlesResource)

      rdd.unpersist(blocking = true)

    })

    // Start streaming engine
    ssc.start()
    ssc.awaitTermination()

  }

  def readFromNifi(ssc: StreamingContext) = {

    val nifiConf = new SiteToSiteClient.Builder()
      .url(nifiEndPoint)
      .portName(nifiPortName)
      .buildConfig()

    ssc.receiverStream(new NiFiReceiver(nifiConf, StorageLevel.MEMORY_ONLY))
      .map(packet => new String(packet.getContent, StandardCharsets.UTF_8))
  }

  def parseGkg(stringRDD: DStream[String]) = {
    stringRDD
      .map(GKGParser.toJsonGKGV2)
      .map(GKGParser.toCaseClass2)
      .filter({ gkg =>
        Try(new URL(gkg.documentId.getOrElse("NA"))).isSuccess
      })
      .filter({ gkg =>
        gkg.v2Locations.getOrElse(Array[Location]()).exists({ location =>
          val lat = location.latitude.getOrElse(Double.MaxValue)
          val lon = location.longitude.getOrElse(Double.MaxValue)
          lat < 71.424593 && lat > 34.986544 && lon < 33.633585 && lon > -13.165017
        })
      })
  }

  def extractDistinctUrls(gkgStream: DStream[GkgEntity2]) = {
    val extractUrlsFromRDD = (rdd: RDD[GkgEntity2]) => {
      rdd.map({ gdelt =>
        gdelt.documentId.getOrElse("NA")
      }).distinct()
    }
    gkgStream.transform(extractUrlsFromRDD)
  }

  def fetchHtml(urlStream: DStream[String]) = {
    urlStream.persist(StorageLevel.DISK_ONLY)
    urlStream.mapPartitions({ it =>
      val html = new HtmlFetcher(gooseConnectionTimeout, gooseSocketTimeout)
      it map html.fetch
    }).filter({ case content =>
      content.body.split("\\s+").length > minWords
    })
  }

  def tokenizeContent(contentStream: DStream[Content], stopwords: Broadcast[Set[String]]) = {
    contentStream.mapPartitions({ it =>
      val analyzer = new EnglishAnalyzer()
      it.map({ case content =>
        val stemmed = Tokenizer.lucene(content.body, analyzer).filter(s => !stopwords.value.contains(s)).mkString(" ")
        Content(content.url, content.title, stemmed)
      })
    })
  }

  def buildTf(contentStream: DStream[Content]) = {
    val buildVectors = (contentRDD: RDD[Content]) => {
      val tfModel = new HashingTF(1 << 20)
      val tfRDD = contentRDD.map(c => (c, tfModel.transform(c.body.split("\\s").toSeq)))
      val normalizer = new Normalizer()
      val sparseVectorRDD = tfRDD.mapValues(v => normalizer.transform(v))
      val embedding = Embedding(Embedding.MEDIUM_DIMENSIONAL_RI)
      sparseVectorRDD.mapValues(v => embedding.embed(v))
    }
    contentStream.transform(buildVectors)
  }

  def extractGeohashes(gkg: GkgEntity2) = {
    gkg.v2Locations.getOrElse(Array[Location]()).map({ location =>
      Try {
        val latitude = location.latitude.get
        val longitude = location.longitude.get
        GeoHash.encode(latitude, longitude)
      }
    }).filter(_.isSuccess).map(_.toOption.get)
  }

  def buildClusterStatistics(rdd: RDD[(String, ((Content, Vector, Int), GkgEntity2))], model: StreamingKMeansModel) = {

    val avgTones = rdd.values.map({ case ((_, _, cId), gkg) =>
      (cId, Try(gkg.tones.get.averageTone.get).getOrElse(0.0d))
    }).groupByKey().mapValues({ it =>
      val list = it.toList
      list.sum / list.size
    }).collectAsMap()

    val topPersons = rdd.values.flatMap({ case ((_, _, cId), gkg) =>
      gkg.v2Persons.getOrElse(Array[Person]()).map(_.person).map(entity => ((cId, entity), 1))
    }).reduceByKey(_ + _).map({ case ((cId, entity), count) =>
      (cId, (entity, count))
    }).groupByKey().mapValues({ it =>
      val list = it.toList
      list.sortBy(_._2).reverse.take(math.min(list.size, 5)).map(_._1)
    }).collectAsMap()

    val topOrgs = rdd.values.flatMap({ case ((_, _, cId), gkg) =>
      gkg.v2Organizations.getOrElse(Array[Organization]()).map(_.organization).map(entity => ((cId, entity), 1))
    }).reduceByKey(_ + _).map({ case ((cId, entity), count) =>
      (cId, (entity, count))
    }).groupByKey().mapValues({ it =>
      val list = it.toList
      list.sortBy(_._2).reverse.take(math.min(list.size, 5)).map(_._1)
    }).collectAsMap()

    val topThemes = rdd.values.flatMap({ case ((_, _, cId), gkg) =>
      gkg.v2Themes.getOrElse(Array[Theme]()).map(_.theme).map(entity => ((cId, entity), 1))
    }).reduceByKey(_ + _).map({ case ((cId, entity), count) =>
      (cId, (entity, count))
    }).groupByKey().mapValues({ it =>
      val list = it.toList
      list.sortBy(_._2).reverse.take(math.min(list.size, 5)).map(_._1)
    }).collectAsMap()

    val topCountries = rdd.values.flatMap({ case ((_, _, cId), gkg) =>
      gkg.v2Locations.getOrElse(Array[Location]()).map(_.country.getOrElse("NA")).map(entity => ((cId, entity), 1))
    }).reduceByKey(_ + _).map({ case ((cId, entity), count) =>
      (cId, (entity, count))
    }).groupByKey().mapValues({ it =>
      val list = it.toList
      list.sortBy(_._2).reverse.take(math.min(list.size, 5)).map(_._1)
    }).collectAsMap()

    val topDocument = rdd.values.map({ case ((content, v, cId), gkg) =>
      (cId, (content, euclideanDistance(model.clusterCenters(cId).toArray, v.toArray)))
    }).groupByKey().mapValues({ it =>
      Try(it.toList.sortBy(_._2).map(_._1).head).toOption
    }).collectAsMap()

    val numArticles = rdd.values.map({ case ((_, _, cId), _) =>
      (cId, 1)
    }).reduceByKey(_ + _).collectAsMap()

    val clusterDates = rdd.values.map({ case ((content, v, cId), gkg) =>
      (cId, gkg.date.getOrElse(new Date().getTime))
    }).groupByKey().mapValues(_.head).collectAsMap()

    (0 to model.clusterCenters.length).map({ cId =>
      val articles = numArticles.getOrElse(cId, 0)
      val date = clusterDates.getOrElse(cId, new Date().getTime)
      val avgTone = avgTones.getOrElse(cId, 0.0d)
      val document = topDocument.get(cId)
      val title = Try(document.get.get.title).getOrElse("")
      val url = Try(document.get.get.url).getOrElse("")
      val countries = topCountries.getOrElse(cId, List[String]())
      val themes = topThemes.getOrElse(cId, List[String]())
      val organizations = topOrgs.getOrElse(cId, List[String]())
      val persons = topPersons.getOrElse(cId, List[String]())
      Cluster(cId, date, articles, title, url, avgTone, themes, countries, organizations, persons)
    }).toList.filter(_.numArticles > 0)
  }

  def euclideanDistance(xs: Array[Double], ys: Array[Double]) = {
    math.sqrt((xs zip ys).map { case (x, y) => math.pow(y - x, 2) }.sum)
  }

  def getMinBatchId(sc: SparkContext) = {
    sc.parallelize(Array(0)).union(sc.esJsonRDD(esNodesResource).values.map({ strJson =>
      implicit val format = DefaultFormats
      val json = parse(strJson)
      (json \ "batch").extractOrElse[Int](0)
    })).max()
  }

  def getMinClusterId(sc: SparkContext) = {
    sc.parallelize(Array(0)).union(sc.esJsonRDD(esNodesResource).values.map({ strJson =>
      implicit val format = DefaultFormats
      val json = parse(strJson)
      (json \ "cid").extractOrElse[Int](0)
    })).max()
  }

  def predictValues(model: StreamingKMeans, gkgContentStream: DStream[(String, (GkgEntity2, Content))], vectorStream: DStream[((String, Seq[String]), Vector)]) = {
    model.predictOnValues(vectorStream).map({ case ((gkgId, corpus), (cluster)) =>
      (gkgId, (corpus, cluster))
    }).join(gkgContentStream)
  }
}
