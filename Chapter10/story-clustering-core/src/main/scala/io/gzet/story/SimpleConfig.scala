package io.gzet.story

import com.typesafe.config.ConfigFactory

import scala.util.Try

trait SimpleConfig {

  lazy val conf = ConfigFactory.load()

  lazy val batchSize = Try(conf.getInt("spark.batchSize")).getOrElse(15)
  lazy val batchWindow = Try(conf.getInt("spark.batchWindow")).getOrElse(3)
  lazy val partitions = Try(conf.getInt("spark.partitions")).getOrElse(10)
  lazy val minWords = Try(conf.getInt("spark.minWords")).getOrElse(150)

  lazy val cassandraHost = Try(conf.getString("cassandra.host")).getOrElse("localhost")
  lazy val cassandraPort = Try(conf.getInt("cassandra.port")).getOrElse(9042)
  lazy val cassandraTable = Try(conf.getString("cassandra.table")).getOrElse("articles")
  lazy val cassandraKeyspace = Try(conf.getString("cassandra.keyspace")).getOrElse("gzet")

  lazy val gooseConnectionTimeout = Try(conf.getInt("goose.connectionTimeout")).getOrElse(10000)
  lazy val gooseSocketTimeout = Try(conf.getInt("goose.socketTimeout")).getOrElse(10000)

  lazy val nifiPortName = Try(conf.getString("nifi.portName")).getOrElse("GKG_Spark_Streaming")
  lazy val nifiEndPoint = Try(conf.getString("nifi.endPoint")).getOrElse("http://localhost:8080/nifi")

  lazy val kMeansClusters = Try(conf.getInt("clustering.kmeans.clusters")).getOrElse(150)
  lazy val kMeansHalfLife = Try(conf.getDouble("clustering.kmeans.halfLife")).getOrElse(2.0d)

  lazy val esNodes = Try(conf.getString("es.nodes")).getOrElse("localhost")
  lazy val esPort = Try(conf.getInt("es.port")).getOrElse(9200)
  lazy val esArticlesResource = Try(conf.getString("es.articlesResource")).getOrElse("gzet/articles")
  lazy val esNodesResource = Try(conf.getString("es.nodesResource")).getOrElse("story/nodes")
  lazy val esEdgesResource = Try(conf.getString("es.edgeResource")).getOrElse("story/edges")

}
