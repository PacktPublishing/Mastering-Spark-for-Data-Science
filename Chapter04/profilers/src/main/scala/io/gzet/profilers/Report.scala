package io.gzet.profilers

object Tags {

  val FIELD_COUNT = "fieldCount"
  val FIELD_IDX = "fieldIdx"
  val FIELD_NAME = "fieldName"
  val MASK = "maskApplied"
  val ASCII_NAME = "asciiValue"
  val ASCII_BINARY = "asciiBinary"
  val EXTRA = "extra"

}

case class Report(
                   metricName: String,
                   metricValue: Double,
                   ingestTime: Long,
                   sources: Array[String],
                   tags: scala.collection.immutable.Map[String, String]
                 )

trait IBuilder {
  def withName(metricName: String): IBuilder

  def withTime(ingestTime: Long): IBuilder

  def withSources(sources: Array[String]): IBuilder

  def withMetric(metric: Double): IBuilder

  def addTag(name: String, value: String): IBuilder

  def withTags(tgs: Map[String, String]): IBuilder

  def build: Report
}

object ReportBuilder {
  def create: IBuilder = Builder()
}

case class Builder(
                    metricName: String = "",
                    metricValue: Double = 0.0d,
                    ingestTime: Long = -1L,
                    sources: Array[String] = Array(),
                    tags: Map[String, String] = Map()
                  ) extends IBuilder {

  val config: Report = Report(metricName, metricValue, ingestTime, sources, tags)

  def withName(name: String) = Builder(name, metricValue, ingestTime, sources, tags)

  def withMetric(metric: Double) = Builder(metricName, metric, ingestTime, sources, tags)

  def withTime(time: Long) = Builder(metricName, metricValue, time, sources, tags)

  def withSources(srcs: Array[String]) = Builder(metricName, metricValue, ingestTime, srcs, tags)

  def withTags(tgs: Map[String, String]) = Builder(metricName, metricValue, ingestTime, sources, tgs)

  def addTag(name: String, value: String) = Builder(metricName, metricValue, ingestTime, sources, tags ++ Map(name -> value))

  def build = config

}