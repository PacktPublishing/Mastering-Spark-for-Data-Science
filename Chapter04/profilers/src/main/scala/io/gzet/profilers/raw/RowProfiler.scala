package io.gzet.profilers.raw

import org.apache.spark.sql.Dataset

case class RowProfiler() {

  def profile(df: Dataset[String]): Dataset[RowReport] = {
    import df.sparkSession.implicits._
    val report = RowReport(df.count().toDouble)
    df.sparkSession.createDataset[RowReport](
      Seq(report)
    )
  }
}

case class RowReport(
                      metricValue: Double
                    )
