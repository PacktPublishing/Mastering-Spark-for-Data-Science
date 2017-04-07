package io.gzet.utils

import java.util.regex.{Matcher, Pattern}
import org.apache.hadoop.io.compress.CryptoCodec
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object DataManipulation {

  val foreNames = Array("John","Fred","Jack","Simon")
  val surNames = Array("Smith","Jones","Hall","West")
  val streets = Array("17 Bound Mews","76 Byron Place","2 The Mall","51 St James")

  def getObfuscationResult(text: String): String = {
    val inputTextArray: Array[String] = text.split(",")
    var returnArray = ArrayBuffer[String]()
    for (i <- 0 to inputTextArray.length - 1) {
      var result = ""
      i match {
        case 0 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case 1 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case 2 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case 3 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case 4 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case 5 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case 6 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case _ => "Unknown field"
      }
      returnArray += result
    }
    returnArray.mkString(",")
  }

  def getMaskedResult(text: String): String = {
    val inputTextArray: Array[String] = text.split(",")
    var returnArray = ArrayBuffer[String]()
    val place = scala.util.Random.nextInt(PlaceTuples.maxId)
    for (i <- 0 to inputTextArray.length - 1) {
      var result = ""
      i match {
        case 0 => result = foreNames(scala.util.Random.nextInt(foreNames.length))
        case 1 => result = surNames(scala.util.Random.nextInt(surNames.length))
        case 2 => result = streets(scala.util.Random.nextInt(surNames.length))
        case 3 => result = PlaceTuples(place).toString.split(",")(0)
        case 4 => result = PlaceTuples(place).toString.split(",")(1)
        case 5 => result = PlaceTuples(place).toString.split(",")(2)
        case 6 => result = getRandomCCNumber
        case _ => "Unknown field"
      }
      returnArray += result
    }
    returnArray.mkString(",")
  }



  def getRandomCCNumber(): String = {
    var returnArray = ArrayBuffer[String]()
    for(i <- 0 to 3) {
      val tupleArray: Array[Int] = new Array[Int](4)
      for (x <- 0 to 3) {
        tupleArray(x) = scala.util.Random.nextInt(9)
      }
      returnArray += tupleArray.mkString
    }
    returnArray.mkString(" ")
  }

  def stringObfuscator(text: String, maskArgs: String, maskChar: Char): String = {

    // 0,x - mask from 0 to x
    // 0,len - mask from 0 to len
    // prefix - mask all but everything after last space
    // suffix - mask all but everything before first space
    // "" do nothing

    var start = 0
    var end = 0

    if(maskArgs == null || maskArgs.equals("")){
      text
    }

    if(maskArgs.contains(",")){
      start = maskArgs.split(',')(0).toInt
      if(maskArgs.split(',')(1) == "len")
        end = text.length
      else
        end = maskArgs.split(',')(1).toInt
    }

    if(maskArgs.contains("prefix")){
      start = 0
      end = text.indexOf(" ") - 1
    }

    if(maskArgs.contains("suffix")){
      start = 0
      end = text.lastIndexOf(" ") + 1
    }

    if(start < 0)
      start = 0

    if( end > text.length() )
      end = text.length()

    if(start > end)
      maskChar

    val maskLength: Int = end - start

    if(maskLength == 0)
      text

    var sbMasked: StringBuilder  = new StringBuilder(maskLength)


    for(i <- 1 to maskLength) {
      sbMasked.append(maskChar)
    }
    text.substring(0, start) + sbMasked.toString() + text.substring(start + maskLength)
  }

  def main(args: Array[String]): Unit = {
    println(getObfuscationResult("John,Smith,3 New Road,London,E1 2AA,0207 123456,4659 4234 5678 9999"))
    println(getMaskedResult("John,Smith,3 New Road,London,E1 2AA,0207 123456,4659 4234 5678 9999"))
    println(getMixedResult("John,Smith,3 New Road,London,E1 2AA,0207 123456,4659 4234 5678 9999"))

    val conf = new SparkConf()
    val sc = new SparkContext(conf.setAppName("Data Test"))

    conf.set("textinputformat.record.delimiter", "\n")
    val dataset = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat]("/input/data/path")
    val data = dataset.map(kvPair => getMixedResult(kvPair._2.toString))
    data.saveAsTextFile("/output/data/path", classOf[CryptoCodec])
  }

  def getMixedResult(text: String): String = {
    val inputTextArray: Array[String] = text.split(",")
    var returnArray = ArrayBuffer[String]()
    val place = scala.util.Random.nextInt(PlaceTuples.maxId)
    for (i <- 0 to inputTextArray.length - 1) {
      var result = ""
      i match {
        case 0 => result = foreNames(scala.util.Random.nextInt(foreNames.length))
        case 1 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case 2 => result = streets(scala.util.Random.nextInt(surNames.length))
        case 3 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case 4 => result = PlaceTuples(place).toString.split(",")(1)
        case 5 => result = stringObfuscator(inputTextArray(i), FieldType(i).toString, '#')
        case 6 => result = getRandomCCNumber
        case _ => "Unknown field"
      }
      returnArray += result
    }
    returnArray.mkString(",")
  }

  def a(line:String): String = {
    ""
  }
}

object FieldType extends Enumeration{
  type fieldId = Value
  val FIRSTNAME  = Value(0, "")
  val SURNAME    = Value(1, "0,len")
  val ADDRESS1   = Value(2, "0,1")
  val ADDRESS2 = Value(3, "")
  val POSTCODE  = Value(4, "suffix")
  val TELNUMBER = Value(5, "suffix")
  val CCNUMBER    = Value(6, "suffix")
}

object PlaceTuples extends Enumeration{
  type place = Value
  val LONDON  = Value(0, "London,NW1 2JT,0171 123890")
  val NEWCAStLE    = Value(1, "Newcastle, N23 2FD,0191 567000")
  val BRISTOL   = Value(2, "Bristol,BS1 2AA,0117 934098")
  val MANCHESTER = Value(3, "Manchester,M56 9JH,0121 111672")
}





