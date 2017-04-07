package io.gzet.timeseries.graph

import io.gzet.test.SparkFunSuite
import org.apache.log4j.{Logger, Level}
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD

import scala.io.Source

class GodwinTest extends SparkFunSuite {

  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  def buildEdges() = {
    Source.fromInputStream(getClass.getResourceAsStream("/edges.csv")).getLines().drop(1).map(s => {
      val Array(source, target, weight) = s.split(",")
      Edge(source.toLong, target.toLong, weight.toDouble)
    }).toList
  }

  localTest("Test Random Walks") { sc =>
    val edges: RDD[Edge[Double]] = sc.parallelize(buildEdges(), 1)
    val godwin = new Godwin(Seq(16))
    val walks = godwin.randomWalks(Graph.fromEdges(edges, 0L), 4).collect().sortBy(_._2)
    println(walks.map(_._1).mkString(" -> "))
    walks.last._1 should be(16)
  }

}
