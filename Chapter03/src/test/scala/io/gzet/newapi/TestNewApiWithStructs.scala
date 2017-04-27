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

import java.io.File

import io.gzet.test.SparkFunSuite
import com.databricks.spark.avro._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row

class TestNewApiWithStructs extends SparkFunSuite {

  val inputFilePath = getClass.getResource("/20160101020000.gkg.csv")
  val avroStructPath = "target/20160101020000.gkg.struct.avro"

  localTest("Create and write Avro using spark-avro lib and Structs") { spark =>
    val gdeltRDD = spark.sparkContext.textFile(inputFilePath.toString)

    val gdeltRowRDD = gdeltRDD.map(_.split("\t", -1))
      .map(attributes => Row(
        CreateAvroWithStructs.createGkgRecordID(attributes(0)),
        attributes(1).toLong,
        attributes(2),
        attributes(3),
        attributes(4),
        CreateAvroWithStructs.createV1Counts(attributes(5)),
        CreateAvroWithStructs.createV21Counts(attributes(6)),
        CreateAvroWithStructs.createV1Themes(attributes(7)),
        CreateAvroWithStructs.createV2EnhancedThemes(attributes(8)),
        CreateAvroWithStructs.createV1Locations(attributes(9)),
        CreateAvroWithStructs.createV2Locations(attributes(10)),
        CreateAvroWithStructs.createV1Persons(attributes(11)),
        CreateAvroWithStructs.createV2Persons(attributes(12)),
        CreateAvroWithStructs.createV1Orgs(attributes(13)),
        CreateAvroWithStructs.createV2Orgs(attributes(14)),
        CreateAvroWithStructs.createV1Stone(attributes(15)),
        CreateAvroWithStructs.createV21Dates(attributes(16)),
        CreateAvroWithStructs.createV2GCAM(attributes(17)),
        attributes(18),
        CreateAvroWithStructs.createV21RelImgAndVid(attributes(19)),
        CreateAvroWithStructs.createV21RelImgAndVid(attributes(20)),
        CreateAvroWithStructs.createV21RelImgAndVid(attributes(21)),
        CreateAvroWithStructs.createV21Quotations(attributes(22)),
        CreateAvroWithStructs.createV21AllNames(attributes(23)),
        CreateAvroWithStructs.createV21Amounts(attributes(24)),
        CreateAvroWithStructs.createV21TransInfo(attributes(25)),
        attributes(26)
      ))

    FileUtils.deleteDirectory(new File(avroStructPath))

    val gdeltDF = spark.createDataFrame(gdeltRowRDD, CreateAvroWithStructs.GkgSchema)
    gdeltDF.write.avro(avroStructPath)

    assertResult(4) (new File(avroStructPath).listFiles.length)

  }

  localTest("Read Avro into Dataframe using spark-avro") { spark =>
    val gdeltAvroDF = spark.read.format("com.databricks.spark.avro").load(avroStructPath)
    assertResult(10) (gdeltAvroDF.count)
    gdeltAvroDF.show
  }
}
