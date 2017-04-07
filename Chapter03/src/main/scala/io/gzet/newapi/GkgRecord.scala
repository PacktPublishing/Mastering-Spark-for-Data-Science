package io.gzet.newapi


case class GkgRecord(gkgRecId: GkgRecordId,
                     v21Date: Long,
                     v2SrcCollectionId: String,
                     V2SrcCmnName: String,
                     v2DocId: String)

//StructField("V2DocId"           , StringType, true),            //$5
//StructField("V1Counts"          , ArrayType(StringType), true) //$6
//    StructField("V21Counts"         , ArrayType(StringType), true), //$7
//    StructField("V1Themes"          , ArrayType(StringType), true), //$8
//    StructField("V2Themes"          , ArrayType(StringType), true), //$9
//    StructField("V1Locations"       , ArrayType(StringType), true), //$10
//    StructField("V2Locations"       , ArrayType(StringType), true), //$11
//    StructField("V1Persons"         , ArrayType(StringType), true), //$12
//    StructField("V2Persons"         , ArrayType(StringType), true), //$13
//    StructField("V1Orgs"            , ArrayType(StringType), true), //$14
//    StructField("V2Orgs"            , ArrayType(StringType), true), //$15
//    StructField("V15Tone"           , StoneStruct, true),           //$16
//    StructField("V21Dates"          , ArrayType(StringType), true), //$17
//    StructField("V2GCAM"            , ArrayType(StringType), true), //$18
//    StructField("V21ShareImg"       , StringType, true),            //$19
//    StructField("V21RelImg"         , ArrayType(StringType), true), //$20
//    StructField("V21SocImage"       , ArrayType(StringType), true), //$21
//    StructField("V21SocVideo"       , ArrayType(StringType), true), //$22
//    StructField("V21Quotations"     , ArrayType(StringType), true), //$23
//    StructField("V21AllNames"       , ArrayType(StringType), true), //$24
//    StructField("V21Amounts"        , ArrayType(StringType), true), //$25
//    StructField("V21TransInfo"      , TranslationInfoStruct, true), //$26
//    StructField("V2ExtrasXML"       , StringType, true)             //$27
case class GkgRecordId(Date: Long, Translingual: Boolean, NumberInBatch: Int)

object V2SrcCollectionId extends Enumeration {
  val WEB, CITATIONONLY, CORE, DTIC, JSTOR, NONTEXTUALSOURCE = Value
}