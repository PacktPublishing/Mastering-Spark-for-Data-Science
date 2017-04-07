package io.gzet.newapi

import io.gzet.test.SparkFunSuite
//import com.databricks.spark.avro._

class TestNewApiWithCaseClass extends SparkFunSuite {

//  localTest("Create and write Avro using spark-avro lib") { spark =>
//
//
//    def createGkgRecordId(str: String): GkgRecordId = {
//      if (str != "") {
//        val split = str.split("-")
//        if (split {
//          1
//        }.length > 1) {
//          GkgRecordId(split {
//            0
//          }.toLong, true, split {
//            1
//          }.substring(1).toInt)
//        }
//        else {
//          GkgRecordId(split {
//            0
//          }.toLong, false, split {
//            1
//          }.toInt)
//        }
//      }
//      else {
//        GkgRecordId(0L, false, 0)
//      }
//    }
//
//
//    val gdeltRDD = spark.sparkContext.textFile("/Users/uktpmhallett/Downloads/20160101020000.gkg.csv")
//
//    val gdeltRowRDD = gdeltRDD.map(_.split("\t"))
//
//    val gkgRecordRDD = gdeltRowRDD.map(attributes => GkgRecord(createGkgRecordId(attributes(0)),
//      attributes(1).toLong,
//      "WEB",
//      "hello",
//      "price"))
//
//    val gdeltDF = spark.createDataFrame(gkgRecordRDD)
//    // write to file in Avro format
//    gdeltDF.write.avro("/Users/uktpmhallett/Downloads/avrotestCase")
//
//  }


}
