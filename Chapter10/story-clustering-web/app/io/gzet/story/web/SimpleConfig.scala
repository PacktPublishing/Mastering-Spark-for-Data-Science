package io.gzet.story.web

import com.typesafe.config.ConfigFactory

import scala.util.Try

trait SimpleConfig {

  lazy val conf = ConfigFactory.load()

  lazy val cassandraHost = Try(conf.getString("cassandra.host")).getOrElse("localhost")
  lazy val cassandraPort = Try(conf.getInt("cassandra.port")).getOrElse(9042)
  lazy val cassandraTable = Try(conf.getString("cassandra.table")).getOrElse("articles")
  lazy val cassandraKeyspace = Try(conf.getString("cassandra.keyspace")).getOrElse("gzet")

  lazy val gooseConnectionTimeout = Try(conf.getInt("goose.connectionTimeout")).getOrElse(10000)
  lazy val gooseSocketTimeout = Try(conf.getInt("goose.socketTimeout")).getOrElse(10000)

}
