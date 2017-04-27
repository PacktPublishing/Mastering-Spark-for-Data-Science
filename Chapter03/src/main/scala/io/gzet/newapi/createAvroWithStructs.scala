package io.gzet.newapi

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object CreateAvroWithStructs {

  def GkgSchema = StructType(Array(
    StructField("GkgRecordId", GkgRecordIdStruct, true),                          //$1
    StructField("V21Date", LongType, true),                                       //$2
    StructField("V2SrcCollectionId" , StringType, true),                          //$3
    StructField("V2SrcCmnName"      , StringType, true),                          //$4
    StructField("V2DocId"           , StringType, true),                          //$5
    StructField("V1Counts"          , ArrayType(V1CountStruct), true),            //$6
    StructField("V21Counts"         , ArrayType(V21CountStruct), true),           //$7
    StructField("V1Themes"          , ArrayType(StringType), true),               //$8
    StructField("V2EnhancedThemes"  , ArrayType(V2EnhancedThemeStruct), true),    //$9
    StructField("V1Locations"       , ArrayType(V1LocationStruct), true),         //$10
    StructField("V2Locations"       , ArrayType(V2EnhancedLocationStruct), true), //$11
    StructField("V1Persons"         , ArrayType(StringType), true),               //$12
    StructField("V2Persons"         , ArrayType(V2EnhancedPersonStruct), true),   //$13
    StructField("V1Orgs"            , ArrayType(StringType), true),               //$14
    StructField("V2Orgs"            , ArrayType(V2EnhancedOrgStruct), true),      //$15
    StructField("V1Stone"           , V1StoneStruct, true),                       //$16
    StructField("V21Dates"          , ArrayType(V21EnhancedDateStruct), true),    //$17
    StructField("V2GCAM"            , ArrayType(V2GcamStruct), true),             //$18
    StructField("V21ShareImg"       , StringType, true),                          //$19
    StructField("V21RelImg"         , ArrayType(StringType), true),               //$20
    StructField("V21SocImage"       , ArrayType(StringType), true),               //$21
    StructField("V21SocVideo"       , ArrayType(StringType), true),               //$22
    StructField("V21Quotations"     , ArrayType(V21QuotationStruct), true),       //$23
    StructField("V21AllNames"       , ArrayType(V21NameStruct), true),            //$24
    StructField("V21Amounts"        , ArrayType(V21AmountStruct), true),          //$25
    StructField("V21TransInfo"      , V21TranslationInfoStruct, true),            //$26
    StructField("V2ExtrasXML"       , StringType, true)                           //$27
  ))

  def GkgRecordIdStruct = StructType(Array(
    StructField("Date", LongType),
    StructField("TransLingual", BooleanType),
    StructField("NumberInBatch", IntegerType)))

  def V1CountStruct = StructType(Array(
    StructField("CountType", StringType, true),
    StructField("Count", IntegerType, true),
    StructField("ObjectType", StringType, true),
    StructField("V1Location", V1LocationStruct, true)
  ))

  def V21CountStruct = StructType(Array(
    StructField("V1Count", V1CountStruct, true),
    StructField("CharOffset", IntegerType, true)
  ))

  def V2EnhancedThemeStruct = StructType(Array(
    StructField("V1Theme", StringType, true),
    StructField("Offset", IntegerType, true)
  ))

  def V1LocationStruct = StructType(Array(
    StructField("LocationType", IntegerType, true),
    StructField("FullName", StringType, true),
    StructField("CountryCode", StringType, true),
    StructField("ADM1Code", StringType, true),
    StructField("LocationLatitude", FloatType, true),
    StructField("LocationLongitude", FloatType, true),
    StructField("FeatureId", StringType, true)
  ))

  def V2EnhancedLocationStruct = StructType(Array(
    StructField("V1Location", V1LocationStruct, true),
    StructField("ADM2Code", StringType, true),
    StructField("CharOffset", IntegerType, true)
  ))

  def V2EnhancedPersonStruct = StructType(Array(
    StructField("V1Person", StringType, true),
    StructField("CharOffset", IntegerType, true)
  ))

  def V2EnhancedOrgStruct = StructType(Array(
    StructField("V1Org", StringType, true),
    StructField("CharOffset", IntegerType, true)
  ))

  def V1StoneStruct = StructType(Array(
    StructField("Tone", FloatType, true),
    StructField("PositiveScore", FloatType, true),
    StructField("NegativeScore", FloatType, true),
    StructField("Polarity", FloatType, true),
    StructField("ActivityRefSensity", FloatType, true),
    StructField("SelfGroupRefDensity", FloatType, true),
    StructField("WordCount", IntegerType, true)
  ))

  def V21EnhancedDateStruct = StructType(Array(
    StructField("DateResolution", IntegerType, true),
    StructField("Month", IntegerType, true),
    StructField("Day", IntegerType, true),
    StructField("Year", IntegerType, true),
    StructField("CharOffset", IntegerType, true)
  ))

  def V2GcamStruct = StructType(Array(
    StructField("DictionaryDimId", StringType, true),
    StructField("Score", FloatType, true)
  ))

  def V21QuotationStruct = StructType(Array(
    StructField("Offset", IntegerType, true),
    StructField("CharLength", IntegerType, true),
    StructField("Verb", StringType, true),
    StructField("Quote", StringType, true)
  ))

  def V21NameStruct = StructType(Array(
    StructField("Name", StringType, true),
    StructField("CharOffset", IntegerType, true)
  ))

  def V21AmountStruct = StructType(Array(
    StructField("Amount", LongType, true),
    StructField("Object", StringType, true),
    StructField("Offset", IntegerType, true)
  ))

  def V21TranslationInfoStruct = StructType(Array(
    StructField("Srclc", StringType),
    StructField("Eng", StringType)
  ))


  def createGkgRecordID(str: String): Row = {
    var ret = Row(0L, false, 0)
    if (str != "") {
      val split = str.split("-")
      if (split {
        1
      }.length > 1) {
        ret = Row(split {
          0
        }.toLong, true, split {
          1
        }.substring(1).toInt)
      }
      else {
        ret = Row(split {
          0
        }.toLong, false, split {
          1
        }.toInt)
      }
    }
    ret
  }

  def createV1Counts(str: String) : Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(";").foreach{
      f => val res = createV1Count(f)
        arrayBuffer += res
    }
    arrayBuffer.toArray
  }

  def createV1Count(str: String) : Row = {
    val blocks = str.split("#")
    val v1Location = createV1Location(str, 3)
    var ret = Row("",0,"",v1Location)
    if (blocks.length > 9) {
      ret = Row(blocks {
        0
      }, blocks {
        1
      }.toInt, blocks {
        2
      }, v1Location)
    }
    ret
  }

  def createV1Location(str: String, offset: Int) : Row = {
    var ret = Row(0,"","","",0.0f,0.0f,"")
    val blocks = str.split("#")
    if(blocks.length > 7 + (offset -1)) {
      var loctype = 0
      var loclat = 0.0f
      var loclon = 0.0f

      if (blocks {
        0 + offset
      } != "") {
        loctype = blocks {
          0 + offset
        }.toInt
      }

      if (blocks {
        4 + offset
      } != "") {
        loclat = blocks {
          4 + offset
        }.toFloat
      }

      if (blocks {
        5 + offset
      } != "") {
        loclon = blocks {
          5 + offset
        }.toFloat
      }

      ret = Row(loctype, blocks {
        1 + offset
      }, blocks {
        2 + offset
      }, blocks {
        3 + offset
      }, loclat, loclon, blocks {
        6 + offset
      })
    }
    ret
  }

  def createV21Counts(str: String) : Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(";").foreach{
      f => if(f != ""){
        val v1Count = createV1Count(f)
        arrayBuffer += Row(v1Count, f.substring(f.lastIndexOf('#') + 1).toInt)
      }
    }
    arrayBuffer.toArray
  }

  def createV1Themes(str: String) : Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]
    str.split(";").foreach(f => arrayBuffer += f)
    arrayBuffer.toArray
  }

  def createV2EnhancedThemes(str: String) : Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(";").foreach {
      f => if (f != "") {
        val s = f.split(",")
        arrayBuffer += Row(s {
          0
        }, s {
          1
        }.toInt)
      }
    }
    arrayBuffer.toArray
  }

  def createV1Locations(str: String) : Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(";").foreach{
      f => val location = createV1Location(f, 0)
        arrayBuffer += location
    }
    arrayBuffer.toArray
  }

  def  createV2Locations(str: String) : Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    val allLocations = str.split(";")
    allLocations.foreach {
      f => val blocks = f.split("#")
        if (blocks.length == 8) {
          var loclat = 0.0f
          var loclon = 0.0f

          if (blocks {
            5
          } != "") {
            loclat = blocks {
              5
            }.toFloat
          }
          if (blocks {
            6
          } != "") {
            loclon = blocks {
              6
            }.toFloat
          }
          arrayBuffer += Row(Row(blocks {
            0
          }, blocks {
            1
          }, blocks {
            2
          }, blocks {
            3
          }, loclat, loclon, blocks {
            7
          }), blocks {
            4
          }, blocks {
            8
          })
        }
    }

    arrayBuffer.toArray
  }

  def createV1Persons(str: String) : Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]
    str.split(";").foreach(f => arrayBuffer += f)
    arrayBuffer.toArray
  }

  def createV2Persons(str: String): Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(";").foreach(f => arrayBuffer += createEnhancedPerson(f))
    arrayBuffer.toArray
  }

  def createEnhancedPerson(str: String): Row = {
    val parts = str.split(",")
    var ret = Row("", 0)
    if (parts.length == 2) {
      ret = Row(parts {
        0
      }, parts {
        1
      }.toInt)
    }
    ret
  }

  def createV1Orgs(str: String): Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]
    str.split(";").foreach(f => arrayBuffer += f)
    arrayBuffer.toArray
  }

  def createV2Orgs(str: String) : Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(";").foreach(f => arrayBuffer += createEnhancedOrganisation(f))
    arrayBuffer.toArray
  }

  def createEnhancedOrganisation(str: String): Row = {
    var ret = Row("", 0)
    val split = str.split(",")
    if(split.length == 2){
      ret = Row(split{0}, split{1}.toInt)
    }
    ret
  }

  def createV1Stone(str: String): Row = {
    var ret = Row(0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0)
    if(str != ""){
      val split = str.split(",")
      ret = Row(split{0}.toFloat,split{1}.toFloat,split{2}.toFloat,split{3}.toFloat,split{4}.toFloat,split{5}.toFloat,split{6}.toInt)
    }
    ret
  }

  def createV21Dates(str: String): Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(";").foreach(f => arrayBuffer += createEnhancedDate(f))
    arrayBuffer.toArray
  }

  def createEnhancedDate(str: String): Row = {
    var ret = Row(0, 0, 0, 0, 0)
    val split = str.split(",")
    if(split.length == 5){
      ret = Row(split{0}.toInt,split{1}.toInt,split{2}.toInt,split{3}.toInt,split{4}.toInt)
    }
    ret
  }

  def createV2GCAM(str: String): Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(",").foreach(f => arrayBuffer += createGcam(f))
    arrayBuffer.toArray
  }

  def createGcam(str: String): Row = {
    var ret = Row("", 0.0f)
    val split = str.split(":")
    if(split.length == 2){
      ret = Row(split{0},split{1}.toFloat)
    }
    ret
  }

  def createV21RelImgAndVid(str: String): Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]
    str.split(";").foreach(f => arrayBuffer += f)
    arrayBuffer.toArray
  }

  def createV21Quotations(str: String): Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split("#").foreach(f => arrayBuffer += createQuotation(f))
    arrayBuffer.toArray
  }

  def createQuotation(str: String): Row = {
    var ret = Row(0, 0, "", "")
    val split = str.split("\\|")
    if(split.length == 4){
      ret = Row(split{0}.toInt, split{1}.toInt, split{2}, split{3})
    }
    ret
  }

  def createV21AllNames(str: String): Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(";").foreach(f => arrayBuffer += createName(f))
    arrayBuffer.toArray
  }

  def createName(str: String): Row = {
    var ret = Row("", 0)
    val split = str.split(",")
    if(split.length == 2){
      ret = Row(split{0}, split{1}.toInt)
    }
    ret
  }

  def createV21Amounts(str: String): Array[Row] = {
    val arrayBuffer = new ArrayBuffer[Row]
    str.split(";").foreach(f => arrayBuffer += createAmount(f))
    arrayBuffer.toArray
  }

  def createAmount(str: String): Row = {
    var ret = Row(0L, "", 0)
    val split = str.split(",")
    if(split.length == 3){
      ret = Row(split{0}.toLong, split{1}, split{2}.toInt)
    }
    ret
  }

  def createV21TransInfo(str: String): Row = {
    var ret = Row("", "")
    val split = str.split(";")
    if(split.length == 2){
      ret = Row(split{0}, split{1})
    }
    ret
  }
}
