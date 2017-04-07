package io.gzet.timeseries

import java.sql.Timestamp

import com.cloudera.sparkts.{DateTimeIndex, TimeSeriesRDD}
import io.gzet.timeseries.timely.MetricImplicits._
import io.gzet.timeseries.timely.TimelyImplicits._
import io.gzet.timeseries.twitter.Twitter._
import io.gzet.utils.spark.accumulo.AccumuloConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Minutes, Period}

object TwitterBatchTimely extends SimpleConfig {

  case class Observation(
                          hashtag: String,
                          time: Timestamp,
                          count: Double
                        )

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("Twitter Extractor")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val twitterJsonRDD = sc.textFile("file:///Users/antoine/CHAPTER/twitter-trump", 500)
    val tweetRDD = twitterJsonRDD mapPartitions analyzeJson cache()

    // Publish metrics to Timely
    tweetRDD.count()
    tweetRDD.countByState.publish()
    tweetRDD.sentimentByState.publish()

    // Read metrics from Timely
    val conf = AccumuloConfig("GZET", "alice", "alice", "localhost:2181")
    val metricsRDD = sc.timely(conf, Some("io.gzet.count"))

    val minDate = metricsRDD.map(_.time).min()
    val maxDate = metricsRDD.map(_.time).max()

    class TwitterFrequency(val minutes: Int) extends com.cloudera.sparkts.PeriodFrequency(Period.minutes(minutes)) {
      def difference(dt1: DateTime, dt2: DateTime): Int = Minutes.minutesBetween(dt1, dt2).getMinutes / minutes
      override def toString: String = s"minutes $minutes"
    }

    val dtIndex = DateTimeIndex.uniform(minDate, maxDate, new TwitterFrequency(1))

    val metricsDF = metricsRDD.filter({
      metric =>
        metric.tags.keys.toSet.contains("tag")
    }).flatMap({
      metric =>
        metric.tags map {
          case (k, v) =>
            ((v, roundFloorMinute(metric.time, 1)), metric.value)
        }
    }).reduceByKey(_+_).map({
      case ((metric, time), sentiment) =>
        Observation(metric, new Timestamp(time), sentiment)
    }).toDF()

    val tsRDD = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, metricsDF, "time", "hashtag", "count").filter(_._2.toArray.exists(!_.isNaN))

  }

  def roundFloorMinute(time: Long, windowMinutes: Int) = {
    val dt = new DateTime(time)
    dt.withMinuteOfHour((dt.getMinuteOfHour / windowMinutes) * windowMinutes).minuteOfDay().roundFloorCopy().toDate.getTime
  }

}
