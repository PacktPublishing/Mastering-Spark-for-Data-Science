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

package io.gzet.profilers.field

import io.gzet.profilers.Utils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scalaz.Scalaz._

case class CardinalityProfiler(topN: Int = 5) {

  def profile(df: Dataset[Array[String]]): Dataset[CardinalityReport] = {

    val total = df.sparkSession.sparkContext.broadcast(df.count())

    import df.sparkSession.implicits._

    val features = Utils.buildColumns(df)

    val topNValues = features.groupByKey({ field =>
      field
    }).count().map({ case (field, count) =>
      (field.idx, Map(field.value -> count))
    }).groupByKey({ case (column, map) =>
      column
    }).reduceGroups({ (v1, v2) =>
      val m1 = v1._2
      val m2 = v2._2
      val m = (m1 |+| m2).toSeq.sortBy(_._2).reverse
      (v1._1, m.take(math.min(m.size, topN)).toMap)
    }).map({ case (column, (_, map)) =>
      val top = map.keySet.toArray
      (column, top)
    })
      .withColumnRenamed("_1", "_topNValues_")
      .withColumnRenamed("_2", "description")

    val cardinalities = features.distinct().groupByKey(_.idx).count().map({
      case (column, distinctValues) =>
        val cardinality = distinctValues / total.value.toDouble
        (column, cardinality)
    })
      .withColumnRenamed("_1", "column")
      .withColumnRenamed("_2", "cardinality")

    cardinalities.join(topNValues, col("column") === col("_topNValues_"))
      .drop("_topNValues_")
      .map({ case Row(column: Int, cardinality: Double, description: mutable.WrappedArray[String]) =>
        CardinalityReport(
          column,
          cardinality,
          description.toArray
        )
      })

  }

}

case class CardinalityReport(
                              field: Int,
                              metricValue: Double,
                              description: Array[String]
                            )
