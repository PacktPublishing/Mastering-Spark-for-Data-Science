package io.gzet.timeseries.timely

import io.gzet.utils.spark.accumulo.AccumuloConfig
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, InputFormatBase}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

object TimelyImplicits {

  implicit class AccumuloReader(sc: SparkContext) {

    def timely(accumuloConfig: AccumuloConfig, rowPrefix: Option[String] = None): RDD[Metric] = {

      val conf = sc.hadoopConfiguration
      val job = Job.getInstance(conf)
      val clientConfig: ClientConfiguration = new ClientConfiguration()
        .withInstance(accumuloConfig.accumuloInstance)
        .withZkHosts(accumuloConfig.zookeeperHosts)

      val authorizations = new Authorizations(List("INTERNAL", "CONFIDENTIAL", "SECRET").map(_.getBytes()))

      AbstractInputFormat.setConnectorInfo(job, accumuloConfig.accumuloUser, new PasswordToken(accumuloConfig.accumuloPassword))
      AbstractInputFormat.setZooKeeperInstance(job, clientConfig)
      AbstractInputFormat.setScanAuthorizations(job, authorizations)
      InputFormatBase.setInputTableName(job, "timely.metrics")

      if(rowPrefix.isDefined) {
        val ranges = List(Range.prefix(rowPrefix.get))
        InputFormatBase.setRanges(job, ranges)
      }

      val rdd = sc.newAPIHadoopRDD(job.getConfiguration,
        classOf[AccumuloTimelyInputFormat],
        classOf[NullWritable],
        classOf[TimelyWritable]
      ) values

      rdd map {
        timely =>
          val Array(tagK, tagV) = timely.getMetricType.split("=", 2)
          Metric(
            timely.getMetric,
            timely.getTime,
            timely.getMetricValue,
            Map(tagK -> tagV)
          )
      }
    }
  }

}
