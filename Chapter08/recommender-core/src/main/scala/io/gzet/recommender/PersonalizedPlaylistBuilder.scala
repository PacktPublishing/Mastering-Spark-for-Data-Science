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

import com.typesafe.config.Config
import io.gzet.recommender.Config._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import spark.jobserver._

object PersonalizedPlaylistBuilder extends SparkJob with NamedRddSupport {

  override def runJob(sc: SparkContext, conf: Config): Any = {

    val id = conf.getLong("song.id")

    val edges = this.namedRdds.get[Edge](RDD_EDGE).get
    val nodes = this.namedRdds.get[Node](RDD_NODE).get

    val edgeRDD = edges.flatMap({e =>
      e.targets.zip(e.weights).map({case (target, weight) =>
        org.apache.spark.graphx.Edge(e.source, target.toLong, weight.toDouble)
      })
    })

    val songIdsB = sc.broadcast(nodes.map(n => (n.id, n.name)).collectAsMap())

    val graph = Graph.fromEdges(edgeRDD, 0L)
    graph.cache()
    val prGraph = graph.personalizedPageRank(id, TOLERANCE, TELEPORT)

    prGraph.vertices.mapPartitions({ it =>
      val songIds = songIdsB.value
      it map { case (vId, pr) =>
        (vId, songIds.getOrElse(vId, "UNKNOWN"), pr)
      }
    }).sortBy(_._3, ascending = false).map(v => List(v._1, v._3, v._2).mkString(",")).collect()

  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    if(!config.hasPath("song.id")) return SparkJobInvalid("Missing parameter [song.id]")
    if(this.namedRdds.get[Edge](RDD_EDGE).isEmpty) return SparkJobInvalid("Missing RDD [edges]")
    if(this.namedRdds.get[Edge](RDD_NODE).isEmpty) return SparkJobInvalid("Missing RDD [nodes]")
    SparkJobValid
  }

}
