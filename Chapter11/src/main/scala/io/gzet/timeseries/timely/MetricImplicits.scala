package io.gzet.timeseries.timely

import java.io.PrintStream
import java.net.Socket
import java.nio.charset.StandardCharsets

import io.gzet.timeseries.SimpleConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Logging, Partitioner}

object MetricImplicits extends Logging with SimpleConfig {

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  class MetricPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[MetricKey]
      nonNegativeMod(k.metricName.hashCode, partitions)
    }
  }

  implicit class Metrics(rdd: RDD[Metric]) {

    val partitions = rdd.partitions.length
    val partitioner = new MetricPartitioner(partitions)

    def publish() = {
      val sSortedMetricRDD = rdd filter { metric =>
        metric.tags.nonEmpty
      } map { metric =>
        (MetricKey(metric.name, metric.time), metric)
      } repartitionAndSortWithinPartitions partitioner

      sSortedMetricRDD.values foreachPartition { it: Iterator[Metric] =>
        val sock = new Socket(timelyHost, timelyPort)
        val writer = new PrintStream(sock.getOutputStream, true, StandardCharsets.UTF_8.name)
        it foreach { metric =>
          writer.println(metric.toPut)
        }
        writer.flush()
      }
    }
  }


  implicit class MetricStream(stream: DStream[Metric]) {
    def publish() = {
      stream foreachRDD {
        rdd => rdd.publish()
      }
    }
  }
}

case class Metric(name: String, time: Long, value: Double, tags: Map[String, String], viz: Option[String] = None) {
  def toPut = {
    val vizMap = if(viz.isDefined) List("viz" -> viz.get) else List[(String, String)]()
    val strTags = vizMap.union(tags.toList).map({ case (k, v) =>
      s"$k=$v"
    }).mkString(" ")
    s"put $name $time $value $strTags"
  }
}

case class MetricKey(metricName: String, metricTime: Long)

object MetricKey {
  implicit def orderingByMetricDate[A <: MetricKey] : Ordering[A] = {
    Ordering.by(fk => (fk.metricName, fk.metricTime))
  }
}