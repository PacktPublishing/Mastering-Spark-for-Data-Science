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

package io.gzet.community

import com.typesafe.config.ConfigFactory
import io.gzet.community.accumulo.{EdgeWritable, AccumuloReader, AccumuloConfig}
import io.gzet.community.clustering.Clustering
import io.gzet.community.clustering.louvain.LouvainDetection
import io.gzet.community.clustering.wcc.WCCDetection
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GzetCommunities {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) = {

    if(args.length < 2)
      throw new Exception("usage: <wcc|louvain> <outputDir> <authorization>")

    val algorithm = args.head
    val outputDir = args(1)
    val authorization = if(args.length == 3) {
      Some(args.last)
    } else {
      None: Option[String]
    }

    val spark = SparkSession.builder()
      .appName("communities")
      .getOrCreate()

    val sc = spark.sparkContext

    val config = ConfigFactory.load()

    val partitions = config.getInt("io.gzet.community.partitions")
    val sigmaWCC = config.getDouble("io.gzet.community.wcc.sigma")
    val maxIterationsWCC = config.getInt("io.gzet.community.wcc.maxIterations")
    val minChangedWCC = config.getInt("io.gzet.community.wcc.minChanged")
    val minChangedLouvain = config.getInt("io.gzet.community.louvain.minChanged")
    val minIterationsLouvain = config.getInt("io.gzet.community.louvain.minIterations")

    val accumuloTable = config.getString("io.gzet.accumulo.table")
    val accumuloConf = AccumuloConfig(
      config.getString("io.gzet.accumulo.instance"),
      config.getString("io.gzet.accumulo.user"),
      config.getString("io.gzet.accumulo.password"),
      config.getString("io.gzet.accumulo.zookeeper")
    )

    val reader = new AccumuloReader(accumuloConf)
    val personsRdd = reader.read(sc, accumuloTable, authorization)
    personsRdd.cache()

    val dictionary = buildDictionary(personsRdd)
    dictionary.cache()
    dictionary.count()

    val edges = buildEdges(personsRdd, dictionary)
    val canonicalGraph = Graph.fromEdges(edges, 0L)

    canonicalGraph.cache()
    canonicalGraph.vertices.count()

    personsRdd.unpersist(blocking = false)

    val clustering: Clustering = {
      algorithm match {
        case "louvain"  => new LouvainDetection(minChangedLouvain, minIterationsLouvain)
        case "wcc"      => new WCCDetection(partitions, maxIterationsWCC, sigmaWCC, minChangedWCC)
        case _ =>
          throw new Exception(s"Unsupported algorithm [$algorithm]")
      }
    }

    val communityRdd = clustering.run(canonicalGraph, sc)
    val communityGraph = canonicalGraph.outerJoinVertices(communityRdd)((vId, vData, cId) => cId.getOrElse(vId))

    communityGraph.cache()
    communityGraph.vertices.count()
    canonicalGraph.unpersist(blocking = false)

    val keep = sc.broadcast(communityGraph.vertices.map(_._2 -> 1).reduceByKey(_+_).filter(_._2 > 3).collectAsMap())

    val keepGraph = communityGraph.subgraph((e: EdgeTriplet[Long, Long]) =>
      keep.value.contains(e.srcAttr) || keep.value.contains(e.dstAttr))

    keepGraph.vertices.join(dictionary.map(_.swap)).map({case (vId, (cId, person)) =>
      List(vId, cId, person).mkString(",")
    }).repartition(1).saveAsTextFile(s"$outputDir/nodes")

    keepGraph.edges.repartition(1).map(e => e.srcId + "," + e.dstId).saveAsTextFile(s"$outputDir/edges")

  }

  def buildEdges(edgeRdd: RDD[EdgeWritable], personsId: RDD[(String, Long)]): RDD[Edge[Long]] = {
    edgeRdd map { edge =>
      (edge.getSourceVertex, edge)
    } join personsId map { case (from, (edge, fromId)) =>
      (edge.getDestVertex, (fromId, edge))
    } join personsId map { case (to, ((fromId, edge), toId)) =>
      Edge(fromId, toId, edge.getCount.toLong)
    }
  }

  def buildDictionary(personsRdd: RDD[EdgeWritable]) = {
    personsRdd flatMap { edge =>
      List(edge.getSourceVertex, edge.getDestVertex)
    } distinct() zipWithIndex() mapValues { index =>
      index + 1L
    }
  }
}
