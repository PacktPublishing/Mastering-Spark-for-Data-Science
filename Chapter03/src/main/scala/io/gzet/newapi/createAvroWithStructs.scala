package io.gzet.newapi


import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object createAvroWithStructs {

  def GkgSchema = StructType(Array(
    StructField("GkgRecordId", GkgRecordIdStruct, true),                          //$1
    StructField("V21Date", LongType, true),                                       //$2
    StructField("V2SrcCollectionId" , StringType, true),                          //$3
    StructField("V2SrcCmnName"      , StringType, true),                          //$4
    StructField("V2DocId"           , StringType, true),                          //$5
    StructField("V1Counts"          , ArrayType(StringType), true),            //$6
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
    if (str != "") {
      val split = str.split("-")
      if (split {
        1
      }.length > 1) {
        Row(split {
          0
        }.toLong, true, split {
          1
        }.substring(1).toInt)
      }
      else {
        Row(split {
          0
        }.toLong, false, split {
          1
        }.toInt)
      }
    }
    else {
      Row(0L, false, 0)
    }
  }
}
