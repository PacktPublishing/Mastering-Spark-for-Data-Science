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

import io.gzet.profilers.Utils._
import org.apache.spark.sql.{Dataset, Row}

case class AsciiProfiler(asciiMap: Map[Int, Ascii]) {

  def profile(df: Dataset[String]): Dataset[AsciiReport] = {

    val charset = df.sparkSession.sparkContext.broadcast(asciiMap)

    import df.sparkSession.implicits._

    val charCount = df.flatMap(_.toCharArray.map(_.asInstanceOf[Int]))
      .groupByKey(t => t)
      .count()
      .withColumnRenamed("value", "tmp")
      .withColumnRenamed("count(1)", "count")

    charCount.map({ case Row(octet: Int, count: Long) =>
      val ascii = charset.value.getOrElse(octet, Ascii("NA", "NA", "NA", "NA", "NA"))
      AsciiReport(
        ascii.binary,
        ascii.description,
        count
      )
    })
  }
}

object AsciiProfiler {
  def apply(): AsciiProfiler = {
    val ascii = readAs[Ascii]("/ascii.csv").toList
    AsciiProfiler(ascii.map(a => (a.octet.toInt, a)).toMap)
  }
}

case class Ascii(
                  symbol: String,
                  octet: String,
                  hex: String,
                  binary: String,
                  description: String
                )

case class AsciiReport(
                        binary: String,
                        ascii: String,
                        metricValue: Double
                      )

