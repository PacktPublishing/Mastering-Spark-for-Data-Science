package io.gzet

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.compress.CryptoCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, FunSuite}

class CryptoTest extends FunSuite with Matchers {

  val cryptoDir = System.getProperty("java.io.tmpdir") + "cryptTestDir"

  test("Crypto encrypt then decrypt file") {
    val conf = new SparkConf()
      .setAppName("Test Crypto")
      .setMaster("local")
      .set("spark.default.parallelism", "1")
      .set("spark.hadoop.io.compression.codecs", "org.apache.hadoop.io.compress.CryptoCodec")
    val sc = new SparkContext(conf)

    val testFile = getClass.getResource("/gdeltTestFile.csv")
    val rdd = sc.textFile(testFile.getPath)

    rdd.saveAsTextFile(cryptoDir, classOf[CryptoCodec])
    val read = sc.textFile(cryptoDir)

    val allLines = read.collect
    allLines.size should be(20)
    allLines(0).startsWith("331150686") should be (true)
    allLines(allLines.length - 1).endsWith("polytrack/") should be (true)

    FileUtils.deleteDirectory(new File(cryptoDir))
    sc.stop
  }
}



