package io.gzet.newapi

import java.io.File

import io.gzet.newapi.CreateAvroWithCase.{V21EnhancedDate, GkgRecordCase}
import io.gzet.test.SparkFunSuite
import com.databricks.spark.avro._
import org.apache.commons.io.FileUtils

class TestNewApiWithCaseClass extends SparkFunSuite {

  val inputFilePath = getClass.getResource("/20160101020000.gkg.csv")
  val avroStructPath = "target/20160101020000.gkg.case.avro"

  localTest("Create and write Avro using spark-avro lib and case") { spark =>

    val gdeltRDD = spark.sparkContext.textFile(inputFilePath.toString)

    val gdeltRowRDD = gdeltRDD.map(_.split("\t", -1))

    val gkgRecordRDD = gdeltRowRDD.map(attributes =>
      GkgRecordCase(CreateAvroWithCase.createGkgRecordId(attributes(0)),
      attributes(1).toLong,
      attributes(2),
      attributes(3),
      attributes(4),
      CreateAvroWithCase.createV1Counts(attributes(5)),
      CreateAvroWithCase.createV21Counts(attributes(6)),
      CreateAvroWithCase.createV1Themes(attributes(7)),
      CreateAvroWithCase.createV2EnhancedThemes(attributes(8)),
      CreateAvroWithCase.createV1Locations(attributes(9)),
      CreateAvroWithCase.createV2Locations(attributes(10)),
      CreateAvroWithCase.createV1Persons(attributes(11)),
      CreateAvroWithCase.createV2Persons(attributes(12)),
      CreateAvroWithCase.createV1Orgs(attributes(13)),
      CreateAvroWithCase.createV2Orgs(attributes(14)),
      CreateAvroWithCase.createV1Stone(attributes(15)),
      CreateAvroWithCase.createEnhancedDate((attributes(16))),
      CreateAvroWithCase.createV2GCAM(attributes(17)),
      attributes(18),
      CreateAvroWithCase.createV21RelImgAndVid(attributes(19)),
      CreateAvroWithCase.createV21RelImgAndVid(attributes(20)),
      CreateAvroWithCase.createV21RelImgAndVid(attributes(21)),
      CreateAvroWithCase.createV21Quotations(attributes(22)),
      CreateAvroWithCase.createV21AllNames(attributes(23)),
      CreateAvroWithCase.createV21Amounts(attributes(24)),
      CreateAvroWithCase.createV21TransInfo(attributes(25)),
      attributes(26))
    )

    FileUtils.deleteDirectory(new File(avroStructPath))

    val gdeltDF = spark.createDataFrame(gkgRecordRDD)
    gdeltDF.write.avro(avroStructPath)

    assertResult(4) (new File(avroStructPath).listFiles.length)
  }

  localTest("Read Avro into Dataframe using spark-avro") { spark =>
    val gdeltAvroDF = spark.read.format("com.databricks.spark.avro").load(avroStructPath)
    assertResult(10)(gdeltAvroDF.count)

    gdeltAvroDF.show
  }
}
