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

package io.gzet.newapi

import io.gzet.oldapi.createAvro
import io.gzet.test.SparkFunSuite
import com.databricks.spark.avro._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.io.gzet.gdelt.gkg.v1.Location

class TestNewApiWithStructs extends SparkFunSuite {

  localTest("Create and write Avro using spark-avro lib") { spark =>
    val gdeltRDD = spark.sparkContext.textFile("/Users/uktpmhallett/Downloads/20160101020000.gkg.csv")

    def createV1Count(str: String): Array[String]  = {
      Array("a", "b", "c")

    }

    val gdeltRowRDD = gdeltRDD.map(_.split("\t"))
      .map(attributes => Row(
        createAvroWithStructs.createGkgRecordID(attributes(0)),
        attributes(1).toLong,
        attributes(2),
        attributes(3),
        attributes(4),
        createV1Count(attributes(5))
      ))

//          attributes(3),
//          attributes(4),
//          createV1CountArray(attributes(5)),
//            createV21CountArray(attributes(6))
//
//      ))

    val gdeltDF = spark.createDataFrame(gdeltRowRDD, createAvroWithStructs.GkgSchema)
    // write to file in Avro format
    gdeltDF.write.avro("/Users/uktpmhallett/Downloads/avrotest")

  }

//  localTest("Read Avro using spark-avro") { spark =>
//    val gdeltAvroDF = spark.read.format("com.databricks.spark.avro").load("/Users/uktpmhallett/Downloads/avrotest")
//    gdeltAvroDF.show(20)
//  }


}
