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

object PredefinedMasks {

  /**
    * A mask that says  “If the trimmed length
    * of this string is > 0, then output a 1,
    * else a 0.”
    *
    */
  def POP_CHECKS(str: String): Seq[String] = {
    str.trim.length match {
      case 0 => Seq("0")
      case _ => Seq("1")
    }
  }

  /**
    *
    * Lookup based masks for single UTF bytes seen, to swap
    * raw codes for character class labels of my own choosing.
    * Like Japanese bytes = "J"
    *
    */
  def CLASS_FREQS(str: String): Seq[String] = {
    str.map({
      case ch if Character.isIdeographic(ch) => "J"
      case ch if Character.isISOControl(ch) => "^"
      case ch if Character.isWhitespace(ch) => "T"
      case ch if Character.isDigit(ch) => "9"
      case ch if Character.isLetter(ch) => if (ch.isUpper) "A" else "a"
      case ch if Character.isDefined(ch) => "U"
      case everythingElse => everythingElse.toString
    })
  }

  /**
    * UPPER ascii characters => “A”,
    * LOWER ascii characters => “a”,
    * numbers => “9”,
    * all else left as is.
    */
  def ASCIICLASS_HIGHGRAIN(str: String): Seq[String] = {
    Seq(str.replaceAll("[a-z]", "a")
      .replaceAll("[A-Z]", "A")
      .replaceAll("[0-9]", "9")
      .replaceAll("\t", "T"))
  }

  /**
    * All UPPER ascii character blocks => “A”,
    * all LOWER ascii character blocks => “a”,
    * all number blocks => “9”,
    * all else left as is.
    */
  def ASCIICLASS_LOWGRAIN(str: String): Seq[String] = {
    Seq(str.replaceAll("[a-z]+", "a")
      .replaceAll("[A-Z]+", "A")
      .replaceAll("[0-9]+", "9")
      .replaceAll("\t", "T"))
  }

  /**
    * Hex representation (supports 16-bit char codes)
    * i.e. 0041, 0020, 6b45, etc
    *
    */
  def HEX(str: String): Seq[String] = {
    str.map {
      case (i) if i > 65535 =>
        val hchar = (i - 0x10000) / 0x400 + 0xD800
        val lchar = (i - 0x10000) % 0x400 + 0xDC00
        f"$hchar%04x$lchar%04x"
      case (i) if i > 0 => f"$i%04x"
    }
  }

  /**
    * Unicode 16 multi-byte representation
    * i.e. \u0041, \u0020, \u6b45, etc
    *
    */
  def UNICODE(str: String): Seq[String] = {
    str.map {
      case (i) if i > 65535 =>
        val hchar = (i - 0x10000) / 0x400 + 0xD800
        val lchar = (i - 0x10000) % 0x400 + 0xDC00
        f"\\u$hchar%04x\\u$lchar%04x"
      case (i) if i > 0 => f"\\u$i%04x"
    }
  }

}

case class MaskBasedProfiler(topN: Int = 5, maskFunction: String => Seq[String]) {

  def profile(df: Dataset[Array[String]]) = {

    import df.sparkSession.implicits._

    val features = Utils.buildColumns(df).flatMap({ field =>
      maskFunction(field.value).map({ mask =>
        (field.idx, mask)
      })
    })

    val rowsPerMaskPerColumn = features
      .groupByKey({ case (col, mask) => (col, mask) })
      .count()
      .map({ case ((col, mask), count) =>
        (col, mask, count)
      })
      .withColumnRenamed("_1", "col")
      .withColumnRenamed("_2", "mask")
      .withColumnRenamed("_3", "maskCount")

    val topNValues = Utils.buildColumns(df).flatMap({ f =>
      maskFunction(f.value).map({ mask =>
        (f.idx, mask, f.value)
      })
    }).groupByKey({ case (field, mask, value) =>
      (field, mask, value)
    }).count().map({ case ((field, mask, value), count) =>
      ((field, mask), Map(value -> count))
    }).groupByKey({ case ((field, mask), map) =>
      (field, mask)
    }).reduceGroups({ (v1, v2) =>
      val m1 = v1._2
      val m2 = v2._2
      val m = (m1 |+| m2).toSeq.sortBy(_._2).reverse
      (v1._1, m.take(math.min(m.size, topN)).toMap)
    }).map({ case ((field, mask), (_, map)) =>
      val top = map.keySet.toArray
      (field, mask, top)
    })
      .withColumnRenamed("_1", "_maskDescriptionCol_")
      .withColumnRenamed("_2", "_maskDescriptionMask_")
      .withColumnRenamed("_3", "description")

    val joinedData = rowsPerMaskPerColumn
      .join(topNValues, col("mask") === col("_maskDescriptionMask_") && col("col") === col("_maskDescriptionCol_"))
      .drop("_maskDescriptionMask_")
      .drop("_maskDescriptionCol_")

    joinedData.map({ case Row(col: Int, mask: String, maskCount: Long, description: mutable.WrappedArray[String]) =>
      MaskBasedReport(
        mask,
        col,
        maskCount,
        description.toArray
      )
    })
  }
}

case class MaskBasedReport(
                            mask: String,
                            field: Int,
                            metricValue: Double,
                            description: Array[String]
                          )
