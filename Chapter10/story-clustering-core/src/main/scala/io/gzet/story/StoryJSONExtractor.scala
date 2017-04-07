package io.gzet.story

import java.io._
import java.util.Date

import io.gzet.story.util.Tokenizer
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import scala.util.Try

object StoryJSONExtractor extends SimpleConfig with Logging {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Story Extractor")
    val sc = new SparkContext(sparkConf)

    val outputDir = args.head
    val minWeight = Try(args.last.toInt).getOrElse(0)

    val nodes = sc.esJsonRDD(esNodesResource).map({ case (_, strJson) =>
      implicit val format = DefaultFormats
      val json = parse(strJson)
      val title = (json \ "title").extractOrElse[String]("")
      val gid = (json \ "gid").extractOrElse[Int](-1)
      val articles = (json \ "articles").extractOrElse[Int](-1)
      val cid = (json \ "cid").extractOrElse[Int](-1)
      val date = (json \ "date").extractOrElse[Long](0L)
      Array(cid, gid, new Date(date).toString, articles, Tokenizer.lucene(title.replaceAll("\\n", "").replaceAll("\\r", "")).mkString(" ")).mkString(",")
    }).collect()

    val nodesMap = sc.broadcast(sc.esJsonRDD(esNodesResource).map({ case (_, strJson) =>
      implicit val format = DefaultFormats
      val json = parse(strJson)
      val gid = (json \ "gid").extractOrElse[Int](-1)
      val cid = (json \ "cid").extractOrElse[Int](-1)
      (cid, gid)
    }).collectAsMap())

    val edges = sc.esJsonRDD(esEdgesResource).map({ case (_, strJson) =>
      implicit val format = DefaultFormats
      val json = parse(strJson)
      val source = (json \ "source").extractOrElse[Int](-1)
      val target = (json \ "target").extractOrElse[Int](-1)
      val weight = (json \ "weight").extractOrElse[Int](-1)
      (source, target, weight)
    }).filter(_._3 > minWeight).map({ case (source, target, weight) =>
      val mutation = nodesMap.value.getOrElse(source, -1) != nodesMap.value.getOrElse(target, -1)
      Array(source, target, weight, mutation).mkString(",")
    }).collect()

    printToFile(new File(s"$outputDir/nodes")) { p =>
      p.println("id,story,date,articles,label")
      nodes.foreach(p.println)
    }

    printToFile(new File(s"$outputDir/edges")) { p =>
      p.println("source,target,weight,mutation")
      edges.foreach(p.println)
    }
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}
