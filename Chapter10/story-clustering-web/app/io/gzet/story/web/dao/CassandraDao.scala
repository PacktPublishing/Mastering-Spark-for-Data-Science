package io.gzet.story.web.dao

import com.datastax.driver.core.Cluster
import io.gzet.story.model.Article
import io.gzet.story.util.SimhashUtils._
import io.gzet.story.web.SimpleConfig

import scala.collection.JavaConversions._
import scala.language.postfixOps

class CassandraDao() extends SimpleConfig {

  private val cluster = Cluster.builder().addContactPoint(cassandraHost).withPort(cassandraPort).build()
  val session = cluster.connect()

  def count(): Long = {
    val stmt = s"SELECT count(*) FROM $cassandraKeyspace.$cassandraTable;"
    val results = session.execute(stmt).all()
    results map { row =>
      row.getLong(0)
    } head
  }

  def findDuplicates(hash: Int): List[Article] = {
    searchmasks flatMap { mask =>
      val searchHash = mask ^ hash
      val stmt = s"SELECT hash, url, title, body FROM $cassandraKeyspace.$cassandraTable WHERE hash = $searchHash;"
      val results = session.execute(stmt).all()
      results map { row =>
        Article(row.getInt("hash"), row.getString("body"), row.getString("title"), row.getString("url"))
      }
    } toList
  }

}
