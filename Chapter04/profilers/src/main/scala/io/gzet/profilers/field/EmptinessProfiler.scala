package io.gzet.profilers.field

import io.gzet.profilers.Utils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Dataset

import scalaz.Scalaz._

case class EmptinessProfiler() {

  def profile(df: Dataset[Array[String]]): Dataset[EmptinessReport] = {

    import df.sparkSession.implicits._

    val features = Utils.buildColumns(df)

    features.map(f => (f.idx, StringUtils.isNotEmpty(f.value))).groupByKey({ case (column, isNotEmpty) =>
      (column, isNotEmpty)
    }).count().map({ case ((column, isNotEmpty), count) =>
      (column, Map(isNotEmpty -> count))
    }).groupByKey({ case (column, map) =>
      column
    }).reduceGroups({ (v1, v2) =>
      (v1._1, v1._2 |+| v2._2)
    }).map({ case (col, (_, map)) =>
      val emptiness = map.getOrElse(false, 0L) / (map.getOrElse(true, 0L) + map.getOrElse(false, 0L)).toDouble
      EmptinessReport(
        col,
        emptiness
      )
    })

  }

}

case class EmptinessReport(
                            field: Int,
                            metricValue: Double
                          )
