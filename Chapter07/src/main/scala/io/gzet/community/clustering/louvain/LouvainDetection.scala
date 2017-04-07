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

package io.gzet.community.clustering.louvain

import io.gzet.community.clustering.Clustering
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scalaz.Scalaz._

import scala.reflect.ClassTag

/**
  * Taken from https://github.com/Sotera/distributed-louvain-modularity
  */
class LouvainDetection(minProgress: Int = 1, progressCounter: Int = 1) extends Clustering with Serializable {

  var communityGraph = None: Option[Graph[Long, Long]]

  override def run[VD: ClassTag](graph: Graph[VD, Long], sc: SparkContext): VertexRDD[Long] = {
    val initialGraph = initializeLouvainGraph(graph)
    louvainCore(sc, initialGraph)
  }

  private def initializeLouvainGraph[VD: ClassTag](graph: Graph[VD, Long]): Graph[VState, Long] = {

    graph.cache()
    val vertices = graph.vertices.count()
    val edges = graph.edges.count()

    val vertexAggregatedWeight = graph.aggregateMessages((context: EdgeContext[VD, Long, Long]) => {
      context.sendToSrc(context.attr)
      context.sendToDst(context.attr)
    }, (m1: Long, m2: Long) => m1 + m2)

    graph.outerJoinVertices(vertexAggregatedWeight)((vId, data, weightOption) => {
      val state = new VState()
      state.community = vId
      state.changed = false
      state.communitySigmaTot = weightOption.getOrElse(0L)
      state.internalWeight = 0L
      state.nodeWeight = weightOption.getOrElse(0L)
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D)

  }

  private def louvainCore(sc: SparkContext, graph: Graph[VState, Long]): VertexRDD[Long] = {

    var louvainGraph = graph.cache()
    var level = -1
    var q = -1.0
    var halt = false

    do {
      level += 1
      val (currentQ, currentGraph, passes) = louvainIteration(sc, louvainGraph, level)
      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph
      saveLevel(level, louvainGraph)
      if (passes > 2 && currentQ > q + 0.001) {
        q = currentQ
        louvainGraph = compressGraph(louvainGraph)
      } else {
        halt = true
      }
    } while (!halt)
    communityGraph.get.vertices
  }

  private def louvainIteration(sc: SparkContext, graph: Graph[VState, Long], level: Int): (Double, Graph[VState, Long], Int) = {

    var louvainGraph = graph.cache()
    val graphWeight = louvainGraph.vertices.values.map(_.nodeWeight).reduce(_ + _)

    val totalGraphWeight = sc.broadcast(graphWeight)

    var messageRDD: VertexRDD[Map[(Long, Long), Long]] = louvainGraph.aggregateMessages((context: EdgeContext[VState, Long, Map[(Long, Long), Long]]) => {
      context.sendToDst(Map((context.srcAttr.community, context.srcAttr.communitySigmaTot) -> context.attr))
      context.sendToSrc(Map((context.dstAttr.community, context.dstAttr.communitySigmaTot) -> context.attr))
    }, (m1: Map[(Long, Long), Long], m2: Map[(Long, Long), Long]) => {
      m1 |+| m2
    })

    //materializes the messageRDD and caches it in memory
    var activeMessages = messageRDD.cache().count()

    var updated = 0L - minProgress
    var even = false
    var count = 0
    val maxIterations = 100000
    var stop = 0
    var updatedLastPhase = 0L

    do {

      count += 1
      even = !even

      // label each vertex with its best community based on neighboring community information
      val labeledVerts: VertexRDD[VState] = louvainVertJoin(louvainGraph, messageRDD, totalGraphWeight, even).cache()

      // calculate new sigma total value for each community (total weight of each community)
      val communityUpdate: RDD[(VertexId, Long)] = labeledVerts.map({ case (vId, vData) =>
          (vData.community, vData.nodeWeight + vData.internalWeight)
        }).reduceByKey(_ + _).cache()

      // map each vertex ID to its updated community information
      val communityMapping = labeledVerts.map({ case (vId, vData) =>
        (vData.community, vId)
      }).leftOuterJoin(communityUpdate).map({ case (community, (vId, sigmaTot)) =>
        (vId, sigmaTot.getOrElse(0L))
      }).cache()

      // join the community labeled vertices with the updated community info
      val updatedVerts = labeledVerts.leftOuterJoin(communityMapping).map({ case (vId, (vData, sigmaTot)) =>
        vData.communitySigmaTot = sigmaTot.getOrElse(0L)
        (vId, vData)
      }).cache()

      updatedVerts.count()
      labeledVerts.unpersist(blocking = false)
      communityUpdate.unpersist(blocking = false)
      communityMapping.unpersist(blocking = false)

      val prevG = louvainGraph
      louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vId, old, newOpt) => newOpt.getOrElse(old))
      louvainGraph.cache()

      // gather community information from each vertex's local neighborhood
      val oldMsgs = messageRDD
      messageRDD = louvainGraph.aggregateMessages((context: EdgeContext[VState, Long, Map[(Long, Long), Long]]) => {
        context.sendToDst(Map((context.srcAttr.community, context.srcAttr.communitySigmaTot) -> context.attr))
        context.sendToSrc(Map((context.dstAttr.community, context.dstAttr.communitySigmaTot) -> context.attr))
      }, (m1: Map[(Long, Long), Long], m2: Map[(Long, Long), Long]) => {
        m1 |+| m2
      }).cache()

      // materializes the graph by forcing computation
      activeMessages = messageRDD.count()

      oldMsgs.unpersist(blocking = false)
      updatedVerts.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)

      // half of the communities can switch on even cycles
      // and the other half on odd cycles (to prevent deadlocks)
      // so we only want to look for progress on odd cycles (after all vertcies have had a chance to move)
      if (even) updated = 0
      updated = updated + louvainGraph.vertices.filter(_._2.changed).count
      if (!even) {
        if (updated >= updatedLastPhase - minProgress) stop += 1
        updatedLastPhase = updated
      }
    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIterations)))

    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVerts = louvainGraph.vertices.innerJoin(messageRDD)((vId, vData, messages) => {

      var k_i_in = vData.internalWeight
      val sigmaTot = vData.communitySigmaTot.toDouble
      messages.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        if (vData.community == communityId) k_i_in += communityEdgeWeight
      })

      val M = totalGraphWeight.value
      val k_i = vData.nodeWeight + vData.internalWeight
      val q = (k_i_in.toDouble / M) - ((sigmaTot * k_i) / math.pow(M, 2))
      if (q < 0) 0 else q

    })

    val actualQ = newVerts.values.reduce(_ + _)
    (actualQ, louvainGraph, count / 2)

  }

  private def louvainVertJoin(louvainGraph: Graph[VState, Long], messageRDD: VertexRDD[Map[(Long, Long), Long]], totalEdgeWeight: Broadcast[Long], even: Boolean) = {

    louvainGraph.vertices.innerJoin(messageRDD)((vId, vData, messages) => {

      var bestCommunity = vData.community
      val startingCommunityId = bestCommunity
      var maxDeltaQ = BigDecimal(0.0)
      var bestSigmaTot = 0L

      messages.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vData.nodeWeight, vData.internalWeight, totalEdgeWeight.value)
        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))) {
          maxDeltaQ = deltaQ
          bestCommunity = communityId
          bestSigmaTot = sigmaTotal
        }
      })

      // only allow changes from low to high communities on even cycles and high to low on odd cycles
      if (vData.community != bestCommunity && ((even && vData.community > bestCommunity) || (!even && vData.community < bestCommunity))) {
        vData.community = bestCommunity
        vData.communitySigmaTot = bestSigmaTot
        vData.changed = true
      } else {
        vData.changed = false
      }
      vData
    })

  }

  private def q(currCommunityId: Long, testCommunityId: Long, testSigmaTot: Long, edgeWeightInCommunity: Long, nodeWeight: Long, internalWeight: Long, totalEdgeWeight: Long): BigDecimal = {
    val isCurrentCommunity = currCommunityId.equals(testCommunityId)
    val M = BigDecimal(totalEdgeWeight)
    val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity
    val k_i_in = BigDecimal(k_i_in_L)
    val k_i = BigDecimal(nodeWeight + internalWeight)
    val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot)

    var deltaQ = BigDecimal(0.0)
    if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
      deltaQ = k_i_in - (k_i * sigma_tot / M)
    }
    deltaQ
  }

  private def compressGraph(graph: Graph[VState, Long], debug: Boolean = true): Graph[VState, Long] = {

    // aggregate the edge weights of self loops. edges with both src and dst in the same community.
    // WARNING  can not use graph.mapReduceTriplets because we are mapping to new vertexIds
    val internalEdgeWeights = graph.triplets.flatMap(et => {
      if (et.srcAttr.community == et.dstAttr.community) {
        Iterator((et.srcAttr.community, 2 * et.attr)) // count the weight from both nodes  // count the weight from both nodes
      }
      else Iterator.empty
    }).reduceByKey(_ + _)

    // aggregate the internal weights of all nodes in each community
    val internalWeights = graph.vertices.values.map({ vData =>
      (vData.community, vData.internalWeight)
    }).reduceByKey(_ + _)

    // join internal weights and self edges to find new interal weight of each community
    val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({ case (vId, (weight1, weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0L)
      val state = new VState()
      state.community = vId
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = weight1 + weight2
      state.nodeWeight = 0L
      (vId, state)
    }).cache()

    // translate each vertex edge to a community edge
    val edges = graph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) Iterator(new Edge(src, dst, et.attr))
      else Iterator.empty
    }).cache()

    // generate a new graph where each community of the previous
    // graph is now represented as a single vertex
    val compressedGraph = Graph(newVerts, edges).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    // calculate the weighted degree of each node
    val nodeWeights = compressedGraph.aggregateMessages((context: EdgeContext[VState, Long, Long]) => {
      context.sendToSrc(context.attr)
      context.sendToDst(context.attr)
    }, (m1: Long, m2: Long) => m1 + m2)

    // fill in the weighted degree of each node
    val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vId, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      data.communitySigmaTot = weight + data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()

    newVerts.unpersist(blocking = false)
    edges.unpersist(blocking = false)
    louvainGraph

  }

  private def saveLevel(level: Int, graph: Graph[VState, Long]) = {

    graph.cache()
    graph.vertices.count()

    if(communityGraph.isEmpty){

      communityGraph = Some(graph.mapVertices((vId, vState) => vState.community))

    } else {

      val previousLevelRdd = communityGraph.get.vertices.map({case (vId, cId) =>
        (cId, vId)
      })

      val newLevelRdd = graph.vertices.mapValues((vId, vData) =>
        vData.community
      )

      val updatedLevelRdd = previousLevelRdd.leftOuterJoin(newLevelRdd).map({case (previousCommunity, (vId, newCommunity)) =>
        (vId, newCommunity.getOrElse(vId))
      })

      communityGraph = Some(Graph.apply(updatedLevelRdd, communityGraph.get.edges))
    }
  }
}