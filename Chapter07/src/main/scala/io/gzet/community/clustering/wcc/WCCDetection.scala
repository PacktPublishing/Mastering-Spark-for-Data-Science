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

package io.gzet.community.clustering.wcc

import io.gzet.community.clustering.Clustering
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.reflect.ClassTag
import scalaz.Scalaz._

/**
  * - Ref: http://arxiv.org/pdf/1411.0557.pdf
  * - Ref: https://www.dama.upc.edu/publications/fp546prat.pdf
  */
class WCCDetection(partitions: Int, maxIterations: Int = 100, sigma: Double = 0.001, minChanged: Int = 0) extends Clustering with Serializable {

  override def run[VD: ClassTag](graph: Graph[VD, Long], sc: SparkContext): VertexRDD[Long] = {
    val wccGraph = initializeWCCGraph(graph, sc)
    val wccPartitionedGraph = repartitionByCommunity(wccGraph)
    wccIteration(wccPartitionedGraph, sc)
  }

  private def initializeWCCGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], sc: SparkContext): Graph[VState, ED] = {

    val cEdgeRdd: RDD[Edge[ED]] = graph.edges.map({ e =>
      if(e.srcId > e.dstId) {
        Edge(e.dstId, e.srcId, e.attr)
      } else e
    })

    val canonicalGraph = Graph.apply(graph.vertices, cEdgeRdd)
      .partitionBy(PartitionStrategy.EdgePartition2D)

    canonicalGraph.cache()
    canonicalGraph.vertices.count()

    val vertexCount = canonicalGraph.vertices.count()
    val edgeCount = canonicalGraph.edges.count()

    // - Get the number of triangles passing through each vertex
    // - Get degrees of each vertex
    // - Collect the vertex neighbors Ids
    // - Remove edges that are not part of at least one triangle (intersection of collected neighbour is empty)
    val triGraph: Graph[VertexStats, ED] = canonicalGraph.triangleCount().outerJoinVertices(graph.collectNeighborIds(EdgeDirection.Either))((vId, triangles, neighbours) =>
      VertexStats(triangles, 0, neighbours.getOrElse(Array()))
    ).subgraph((triplet: EdgeTriplet[VertexStats, ED]) => {
      triplet.srcAttr.neighbours.intersect(triplet.dstAttr.neighbours).nonEmpty
    }, (vId: VertexId, vStats: VertexStats) => vStats.triangles > 0).partitionBy(PartitionStrategy.EdgePartition2D)

    triGraph.cache()
    triGraph.vertices.count()
    graph.unpersist(blocking = false)

    // - The degrees from the filtered Graph is the unique number of vertices each node is forming a triangle with
    // - Each node starts as being the center of its own community
    val subGraph: Graph[VState, ED] = triGraph.outerJoinVertices(triGraph.degrees)((vId, vStat, degrees) => {
      val state = new VState()
      state.vId = vId
      state.cId = vId
      state.changed = true
      state.txV = vStat.triangles
      state.vtxV = degrees.getOrElse(0)
      state.wcc = degrees.getOrElse(0).toDouble / vStat.triangles
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D)

    subGraph.cache()
    val subVertexCount = subGraph.vertices.count()
    val subEdgeCount = subGraph.edges.count()

    triGraph.unpersist(blocking = false)

    // - Each vertex sends a message with its id, its clustering coefficient, its degree, and its initial community
    // - Vertex chooses its new community to be the id of the neighbour who has the highest clustering coefficient
    val iniGraph = subGraph.outerJoinVertices(subGraph.aggregateMessages((ctx: EdgeContext[VState, ED, Map[Long, VState]]) => {
      ctx.sendToDst(Map(ctx.srcId -> ctx.srcAttr))
      ctx.sendToSrc(Map(ctx.dstId -> ctx.dstAttr))
    }, (m1: Map[Long, VState], m2: Map[Long, VState]) => {
      m1 ++ m2
    }))((vId, vData, messages) => {
      val newState = vData.cloneState
      val candidates = messages.getOrElse(Map()).filter({ case (neighbour, message) =>
        message.wcc > vData.wcc ||
          (message.wcc == vData.wcc && message.vtxV > vData.vtxV) ||
          (message.wcc == vData.wcc && message.vtxV == vData.vtxV && neighbour > vId)
      })

      if (candidates.nonEmpty) {
        val best = candidates.toSeq.sortBy({ case (neighbour, message) =>
          (1.0 / message.wcc, 1.0 / message.vtxV, 1.0 / neighbour)
        }).head
        newState.cId = best._1
      }

      newState
    })

    iniGraph.cache()
    iniGraph.vertices.count()

    // Iteratively broadcast to lower rank vertices the new community - if any change
    // 1) Every community contains a single center vertex and a set of border vertices connected to the center vertex.
    // 2) The center vertex has the highest clustering coefficient of any vertex in the community.
    // 3) Given a center vertex y and a border vertex x in a community, the clustering coefficient of y must be higher than the clustering coefficient of any neighbor z of x that is the center of its own community.

    val pregel = Pregel.apply(iniGraph, new VState(), Int.MaxValue, EdgeDirection.Either)((vId: VertexId, vData: VState, message: VState) => {
      val newState = vData.cloneState
      if (message.vId >= 0L) {
        // If message comes from node -1L => Superstep 0
        if (message.vId == vId) {
          // If message comes from myself, this is an ack to stop spamming people
          newState.changed = false
        } else if (message.cId == message.vId) {
          // Node who sent this message is a center of its own community - I become a border node
          newState.changed = false
          newState.cId = message.cId
        } else {
          // Node who sent message is a border node - I become a center of my own community
          newState.changed = true
          newState.cId = vId
        }
      }
      newState
    }, (triplet: EdgeTriplet[VState, ED]) => {
      val messages = collection.mutable.Map[Long, VState]()
      val sorted = compareState(triplet.srcAttr, triplet.dstAttr)
      val (fromNode, toNode) = (sorted.head, sorted.last)
      if (fromNode.changed) {
        // If node has changed, inform lower rank neighbours about the change and notify myself so that I can stop spamming
        messages.put(fromNode.vId, fromNode)
        messages.put(toNode.vId, fromNode)
      }
      messages.toIterator
    }, (c1: VState, c2: VState) => {
      compareState(c1, c2).head
    }).vertices

    pregel.cache()
    pregel.count()

    val communityMembers = sc.broadcast(pregel.map(t => (t._2.cId, 1)).reduceByKey(_+_).collectAsMap())
    subGraph.outerJoinVertices(pregel)((vId, vData, vDataOpt) => {
      vDataOpt.getOrElse(vData)
    })
  }

  private def repartitionByCommunity[ED: ClassTag](graph: Graph[VState, ED]) = {
    val partitionedEdges = graph.triplets.map({e =>
      val partition = math.abs((e.srcAttr.cId, e.dstAttr.cId).hashCode()) % partitions
      (partition, e)
    }).partitionBy(new HashPartitioner(partitions)).map({pair =>
      Edge(pair._2.srcId, pair._2.dstId, pair._2.attr)
    })
    Graph(graph.vertices, partitionedEdges)
  }

  private def wccIteration[ED: ClassTag](initialGraph: Graph[VState, ED], sc: SparkContext): VertexRDD[Long] = {

    initialGraph.cache()

    val vertices = initialGraph.vertices.count()
    val edges = initialGraph.edges.count()

    var communityStats = getCommunityStatistics(initialGraph)
    val bVertices = sc.broadcast(vertices)
    val bCommunityStats = sc.broadcast(communityStats)

    var itGraph = updateWCCx(initialGraph, bCommunityStats).mapVertices({case (vId, vState) =>
      val newState = vState.cloneState
      newState.changed = false
      newState
    })

    itGraph.cache()
    itGraph.vertices.count()
    initialGraph.unpersist(blocking = false)

    var wcc = computeWCC(itGraph, bCommunityStats, vertices)
    var changed = vertices
    var delta = Double.MaxValue
    var it = 0
    var run = true

    do {

      val bPreviousWcc = sc.broadcast(wcc)
      val bPreviousCommunityStats = sc.broadcast(communityStats)

      val communityDegrees = itGraph.aggregateMessages((ctx: EdgeContext[VState, ED, Map[VertexId, Int]]) => {
        ctx.sendToDst(Map(ctx.srcAttr.cId -> 1))
        ctx.sendToSrc(Map(ctx.dstAttr.cId -> 1))
      }, (e1: Map[VertexId, Int], e2: Map[VertexId, Int]) => {
        e1 |+| e2
      })

      val communityUpdate = itGraph.outerJoinVertices(communityDegrees)((vId, vState, vDegrees) => {
        val newState = vState.cloneState
        val deg = vDegrees.getOrElse(Map())
        val wccIs = deg.keys map { community =>
          val cStat = bPreviousCommunityStats.value.get(community).get
          val dIn = deg.getOrElse(community, 0)
          val dOut = deg.filter(_._1 != community).values.sum
          (community, computeWCCIx(cStat, dIn, dOut, bPreviousWcc.value, bVertices.value))
        }

        val wccR = - wccIs.filter(_._1 == vState.cId).map(_._2).sum
        var wccT = 0.0d
        var bestC = vState.cId
        wccIs filter { case (cId, wccI) =>
          cId != vState.cId
        } foreach { case (cId, wccI) =>
          val aux = wccR + wccI
          if(aux > wccT) {
            wccT = aux
            bestC = cId
          }
        }

        if(wccR > wccT && wccR > 0.0d) {
          newState.changed = true
          newState.cId = vId
        } else {
          if(wccT > 0){
            newState.changed = true
            newState.cId = bestC
          }
        }
        newState
      })

      communityUpdate.cache()
      communityUpdate.vertices.count()

      // Get the new number of elements in each community / density / etc..
      communityStats = getCommunityStatistics(communityUpdate)
      val updatedCommunityStats = sc.broadcast(communityStats)
      val communityUpdateWcc = updateWCCx(communityUpdate, updatedCommunityStats)

      communityUpdateWcc.cache()
      communityUpdateWcc.vertices.count()
      communityUpdate.unpersist(blocking = false)

      // Get new total WCC
      val newWcc = computeWCC(communityUpdateWcc, updatedCommunityStats, vertices)
      delta = newWcc - wcc
      wcc = newWcc
      changed = communityUpdateWcc.vertices.map(_._2).filter(_.changed).count()
      run = delta > sigma && it < maxIterations && changed > minChanged

      it += 1

      if(run){

        itGraph.unpersist(blocking = false)
        val updatedGraph = communityUpdateWcc mapVertices { case (vId, vState) =>
          val newState = vState.cloneState
          newState.changed = false
          newState
        }

        itGraph = repartitionByCommunity(updatedGraph)
        itGraph.cache()
        itGraph.vertices.count()
        communityUpdateWcc.unpersist(blocking = false)

      }

    } while (run)

    val cIds = itGraph.vertices.map(_._2.cId).distinct().count()

    itGraph.vertices.mapValues((vid, vState) =>
      vState.cId
    )
  }

  private def computeWCC[ED: ClassTag](graph: Graph[VState, ED], cStats: Broadcast[Map[VertexId, CommunityStats]], vertices: Long) = {
    graph.vertices.map({case (vId, vState) =>
      (vState.cId, vState.wcc)
    }).reduceByKey(_+_).map({case (cId, wcc) =>
      cStats.value.get(cId).get.r * wcc
    }).sum / vertices
  }

  private def computeWCCIx(stats: CommunityStats, dIn: Int, dOut: Int, wcc: Double, v: Long) = {
    val q = (stats.b - dIn) / stats.r.toDouble
    val t1 = tetha1(stats.r, stats.b - dIn, stats.d, dIn, dOut, wcc, q)
    val t2 = tetha2(stats.r, stats.b - dIn, stats.d, dIn, dOut, wcc, q)
    val t3 = tetha3(stats.r, stats.b - dIn, stats.d, dIn, dOut, wcc, q)
    (dIn * t1 + (stats.r - dIn) * t2 + t3) / v.toDouble
  }

  private def tetha1(r: Int, b: Int, d: Double, dIn: Int, dOut: Int, w: Double, q: Double) = {
    val numerator = (d * (r - 1) + 1 + q) * (dIn - 1) * d
    val denominator = (r + q) * ((r - 1) * (r - 2) * math.pow(d, 3) + (dIn - 1) * d + q * (q - 1) * d * w + q * (q - 1) * w + dOut * w)
    numerator / denominator
  }

  private def tetha2(r: Int, b: Int, d: Double, dIn: Int, dOut: Int, w: Double, q: Double) = {
    val numerator = (r - 1) * (r - 2) * math.pow(d, 3) * ((r - 1) * d + q)
    val denominator = ((r - 1) * (r - 2) * math.pow(d, 3) + q * (q - 1) * w + q * (r - 1) * d * w) * (r + q) * (r - 1 + q)
    - numerator / denominator
  }

  private def tetha3(r: Int, b: Int, d: Double, dIn: Int, dOut: Int, w: Double, q: Double) = {
    val numerator = dIn * (dIn - 1) * d * (dIn + dOut)
    val denominator = (dIn * (dIn - 1) * d + dOut * (dOut - 1) * w + dOut * dIn * w) * (r + dOut)
    numerator / denominator
  }

  implicit val VStateOrdering: Ordering[VState] = Ordering.by({ state =>
    (state.wcc, state.vtxV, state.vId)
  })

  private def compareState(c1: VState, c2: VState) = {
    List(c1, c2).sorted(VStateOrdering.reverse)
  }

  private def getCommunityStatistics[ED: ClassTag](graph: Graph[VState, ED]): Map[VertexId, CommunityStats] = {

    val communityVertices = graph.vertices.map({case (vId, vState) =>
      (vState.cId, 1)
    }).reduceByKey(_+_).collectAsMap()

    val communityEdges = graph.triplets.flatMap({triplet =>
      if(triplet.srcAttr.cId == triplet.dstAttr.cId){
        Iterator((("IN", triplet.srcAttr.cId), 1))
      } else {
        Iterator((("OUT", triplet.srcAttr.cId), 1), (("OUT", triplet.dstAttr.cId), 1))
      }
    }).reduceByKey(_+_).collectAsMap()

    // For each community C, we keep the following statistics:
    // - the size of the community r
    // - the edge density of the community δ
    // - the number of edges b that are in the boundary of the community
    communityVertices.map({case (community, elements) =>
      val intEdges = communityEdges.getOrElse(("IN", community), 0)
      val extEdges = communityEdges.getOrElse(("OUT", community), 0)
      val density = 2 * intEdges / math.pow(elements, 2)
      (community, CommunityStats(elements, density, extEdges))
    }).toMap

  }

  def updateWCCx[ED: ClassTag](graph: Graph[VState, ED], cStats: Broadcast[Map[VertexId, CommunityStats]]) = {

    val communityNeighbours: Graph[(VState, Array[Long]), ED] = graph.outerJoinVertices(graph.aggregateMessages((e: EdgeContext[VState, ED, Array[VertexId]]) => {
      if(e.dstAttr.cId == e.srcAttr.cId){
        e.sendToDst(Array(e.srcId))
        e.sendToSrc(Array(e.dstId))
      }
    }, (e1: Array[VertexId], e2: Array[VertexId]) => {
      e1 ++ e2
    }))((vid, vState, vNeighbours) => {
      (vState, vNeighbours.getOrElse(Array()))
    })

    communityNeighbours.cache()

    val communityTriangles = communityNeighbours.aggregateMessages((ctx: EdgeContext[(VState, Array[Long]), ED, Int]) => {
      if(ctx.srcAttr._1.cId == ctx.dstAttr._1.cId){
        val (smallSet, largeSet) = if (ctx.srcAttr._2.length < ctx.dstAttr._2.length) {
          (ctx.srcAttr._2.toSet, ctx.dstAttr._2.toSet)
        } else {
          (ctx.dstAttr._2.toSet, ctx.srcAttr._2.toSet)
        }
        val it = smallSet.iterator
        var counter: Int = 0
        while (it.hasNext) {
          val vid = it.next()
          if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
            counter += 1
          }
        }
        ctx.sendToSrc(counter)
        ctx.sendToDst(counter)
      }
    }, (e1: Int, e2: Int) => (e1 + e2) / 2)

    communityNeighbours.outerJoinVertices(communityTriangles)((vId, vData, triangles) => {
      val newState = vData._1.cloneState
      val vtxC = vData._2.length
      newState.vtxV_C = newState.vtxV - vtxC
      newState.txC = triangles.getOrElse(0)
      newState.wcc = newState.txC * newState.vtxV / (newState.txV * (cStats.value.get(newState.cId).get.r - 1 + newState.vtxV_C).toDouble)
      newState
    })

  }

  case class VertexStats(triangles: Int, degrees: Int, neighbours: Array[Long])
  case class CommunityStats(r: Int, d: Double, b: Int)

}


