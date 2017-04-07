package io.gzet.story

import io.gzet.story.model.{Content, Article}
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import io.gzet.story.util.SimhashUtils._
import com.datastax.spark.connector._

object StoryBatchDedup extends SimpleConfig with Logging {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Story Extractor")
    val sc = new SparkContext(sparkConf)

    val simhashRDD = sc.cassandraTable[Article]("gzet", "articles").zipWithIndex().map({ case (a, id) =>
      ((id, Content(a.url, a.title, a.body)), a.hash)
    })
    Set(0)

    val duplicateTupleRDD = simhashRDD.flatMap({ case ((id, content), simhash) =>
      searchmasks.map({ mask =>
        (simhash ^ mask, id)
      })
    }).groupByKey()

    val edgeRDD = duplicateTupleRDD.values.flatMap({ it =>
      val list = it.toList
      for (x <- list; y <- list) yield (x, y)
    }).filter({ case (x, y) =>
      x != y
    }).distinct().map({case (x, y) =>
      Edge(x, y, 0)
    })

    val duplicateRDD = Graph.fromEdges(edgeRDD, 0L)
      .connectedComponents()
      .vertices
      .join(simhashRDD.keys)
      .values

    duplicateRDD.sortBy(_._1).collect().foreach({ case (story, content) =>
      println(story + "\t" + content.title)
    })

  }

}
