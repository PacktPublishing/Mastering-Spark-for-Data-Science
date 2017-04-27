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

package io.gzet.profilers.raw

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}

case class StructuralProfiler(delimiter: String = ",") {

  def profile(df: Dataset[String]): Dataset[StructuralReport] = {

    import df.sparkSession.implicits._

    val rows = df.mapPartitions({ lines =>
      val parser = new CSVParser(delimiter.charAt(0))
      lines.map(line => (parser.parseLine(line).length, line))
    })

    val fieldCount = rows.groupByKey({ case (fields, line) =>
      fields
    }).count()
      .withColumnRenamed("value", "fields")
      .withColumnRenamed("count(1)", "count")

    val fieldLine = rows.groupByKey({ case (fields, line) =>
      fields
    }).reduceGroups({ (v1, v2) => v1 }).map({ case (fields, (_, line)) =>
      (fields, line)
    })
      .withColumnRenamed("_1", "_fieldLine_")
      .withColumnRenamed("_2", "line")

    fieldCount.join(fieldLine, col("fields") === col("_fieldLine_"))
      .drop("_fieldLine_")
      .map({ case Row(columns: Int, count: Long, line: String) =>
        StructuralReport(
          columns,
          count,
          line
        )
      })
  }
}

case class StructuralReport(
                             fields: Int,
                             metricValue: Double,
                             description: String
                           )