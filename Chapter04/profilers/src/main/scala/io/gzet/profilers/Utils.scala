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

package io.gzet.profilers

import au.com.bytecode.opencsv.CSVParser
import com.bizo.mighty.csv.{CSVReader, CSVReaderSettings}
import org.apache.spark.sql.Dataset

object Utils {

  case class Field(idx: Int, value: String)

  def readAs[T](filename: String)(implicit settings: CSVReaderSettings, mf: Manifest[T]): Iterator[T] = {
    val is = getClass.getResourceAsStream(filename)
    CSVReader(is)(settings) {
      CSVReader.convertRow[T]
    }
  }

  def split(ds: Dataset[String], delimiter: String = ","): Dataset[Array[String]] = {
    import ds.sparkSession.implicits._
    ds.mapPartitions({ lines =>
      val parser = new CSVParser(delimiter.charAt(0))
      lines map parser.parseLine
    })
  }

  def buildColumns(ds: Dataset[Array[String]]): Dataset[Field] = {
    import ds.sparkSession.implicits._
    ds.flatMap({ values =>
      values.zipWithIndex.map({ case (value, col) =>
        Field(col, value)
      })
    })
  }

}
