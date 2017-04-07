/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gzet.recommender

import com.datastax.spark.connector._
import com.typesafe.config.Config
import io.gzet.recommender.Config._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import spark.jobserver._

object PlaylistBuilder extends SparkJob with NamedRddSupport {

  override def runJob(sc: SparkContext, conf: Config): Any = {

    val recordRDD = sc.cassandraTable[Record](KEYSPACE, TABLE_RECORD)
    val hashRDD = sc.cassandraTable[Hash](KEYSPACE, TABLE_HASH)

    val minSimilarityB = sc.broadcast(MIN_SIMILARITY)
    val songIdsB = sc.broadcast(recordRDD.map(r => (r.id, r.name)).collectAsMap())

    implicit class Crossable[X](xs: Traversable[X]) {
      def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
    }

    val songHashRDD = hashRDD flatMap { hash =>
      hash.songs map { song =>
        ((hash, song), 1)
      }
    }

    val songTfRDD = songHashRDD map { case ((hash, songId), count) =>
      (songId, count)
    } reduceByKey(_+_)

    val songTfB = sc.broadcast(songTfRDD.collectAsMap())

    val crossSongRDD = songHashRDD.keys.groupByKey().values flatMap { songIds =>
      songIds cross songIds filter { case (from, to) =>
        from != to
      } map(_ -> 1)
    } reduceByKey(_+_) map { case ((from, to), count) =>
      val weight = count.toDouble / songTfB.value.getOrElse(from, 1)
      org.apache.spark.graphx.Edge(from, to, weight)
    } filter { edge =>
      edge.attr > minSimilarityB.value
    }

    val graph = Graph.fromEdges(crossSongRDD, 0L)
    val prGraph = graph.pageRank(TOLERANCE, TELEPORT)

    val edges = prGraph.edges.map({ edge =>
      (edge.srcId, (edge.dstId, edge.attr))
    }).groupByKey().map({case (srcId, it) =>
      val dst = it.toList
      val dstIds = dst.map(_._1.toString)
      val weights = dst.map(_._2.toString)
      Edge(srcId, dstIds, weights)
    })

    val vertices = prGraph.vertices.mapPartitions({ vertices =>
      val songIds = songIdsB.value
      vertices map { case (vId, pr) =>
        Node(vId, songIds.getOrElse(vId, "UNKNOWN"), pr)
      }
    })

    edges.saveAsCassandraTable(KEYSPACE, TABLE_EDGE)
    vertices.saveAsCassandraTable(KEYSPACE, TABLE_NODE)

    this.namedRdds.update(RDD_EDGE, edges)
    this.namedRdds.update(RDD_NODE, vertices)

  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }



}
