package io.gzet.timeseries.graph

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths

import scala.collection.immutable.TreeSet

class Godwin(points: Seq[Long]) extends Logging with Serializable {

  private val myOrdering = Ordering.fromLessThan[Int](_ > _)
  private case class VData(hops: TreeSet[Int], active: Boolean, neighbors: Seq[(Long, Double)], toDst: Long)

  private def nextNode(v: VData) = {
    v.neighbors(monteCarlo(v.neighbors.map(_._2).toArray))._1
  }

  private def monteCarlo(distribution: Array[Double], sampleKeyDummy: Double = Double.MinValue, idx: Int = 0): Int = {
    val sampleKey = sampleKeyDummy match {
      case Double.MinValue => scala.util.Random.nextDouble()
      case s: Double => s
    }
    if(distribution.head >= sampleKey) {
      idx
    } else {
      monteCarlo(distribution.tail, sampleKey - distribution.head, idx + 1)
    }
  }

  private def initGraph(graph: Graph[VertexId, Double], seed: Long): Graph[VData, Double] = {

    logInfo("Initializing Graph")
    val vertexRDD = graph.collectEdges(EdgeDirection.Out).mapValues((vid, edges) => {
      val similarities = edges.map({
        edge =>
          if(edge.srcId == vid) {
            (edge.dstId, edge.attr)
          } else {
            (edge.srcId, edge.attr)
          }
      }).toSeq.distinct.sortBy(_._2).reverse

      val neighbors = if(similarities.map(_._2).sum != 1.0d) {
        val tot = similarities.map(_._2).sum
        similarities.map({
          case (vId, sim) => (vId, sim / tot)
        })
      } else {
        similarities
      }

      val set = if(seed == vid) TreeSet.empty(myOrdering) + 0 else TreeSet.empty(myOrdering)
      VData(set, seed == vid, neighbors, -1)
    })

    graph.outerJoinVertices(vertexRDD)((vid, _, vdata) => {
      vdata.get
    })
  }

  def distance(graph: Graph[VertexId, Double]) = {

    val sc = graph.vertices.sparkContext
    val shortestPaths = ShortestPaths.run(graph, points)

    shortestPaths.cache()
    shortestPaths.vertices.count()

    val depth = sc.broadcast(shortestPaths.vertices.values.filter(_.nonEmpty).map(_.values.min).max())
    logInfo(s"Godwin depth is [${depth.value}]")

    shortestPaths.vertices.map({
      case (vid, hops) =>
        if(hops.nonEmpty) {
          (vid, Option(math.min(hops.values.min / depth.value.toDouble, 1.0)))
        } else {
          (vid, None: Option[Double])
        }
    }).filter(_._2.isDefined).map({
      case (vid, distance) =>
        (vid, distance.get)
    })
  }

  def randomWalks(graph: Graph[VertexId, Double], seed: Long, maxIterations: Int = 1000) = {

    val init = initGraph(graph, seed)
    init.cache()
    init.vertices.count()

    val core = (vId: VertexId, v: VData, message: Option[Int]) => {
      if(message.isDefined) { // MESSAGE
        if(message.get >= 0) { // ACTIVE
          VData(v.hops + message.get, active = !points.contains(vId), v.neighbors, nextNode(v))
        } else { // INACTIVE
          VData(v.hops, active = false, v.neighbors, -1)
        }
      } else { // SUPERSTEP
        VData(v.hops, active = v.hops.nonEmpty, v.neighbors, nextNode(v))
      }
    }

    val merge = (v1: Option[Int], v2: Option[Int]) => v1

    val send = (t: EdgeTriplet[VData, Double]) => {
      if(t.srcAttr.active) {
        val message = t.srcAttr.hops.head
        val dstId = t.srcAttr.toDst
        Iterator((t.srcId, Some(-1))) ++ {
          if(t.dstId == dstId) {
            Iterator((t.dstId, Some(message + 1)))
          } else {
            Iterator.empty
          }
        }
      } else { // Inactive node: stay silent
        Iterator.empty
      }
    }

    logInfo(s"Starting Pregel for at most $maxIterations iterations")

    val none = None: Option[Int]
    Pregel.apply(init, none, maxIterations, EdgeDirection.Either)(core, send, merge).vertices.flatMap({
      case (vid, vdata) =>
        vdata.hops.map({
          hop =>
            (vid, hop)
        })
    })

  }
}
