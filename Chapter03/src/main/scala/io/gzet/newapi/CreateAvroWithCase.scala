package io.gzet.newapi

import scala.collection.mutable.ArrayBuffer

object CreateAvroWithCase {

  case class GkgRecordCase(gkgRecId: gkgRecordId,
                           v21Date: Long,
                           v2SrcCollectionId: String,
                           V2SrcCmnName: String,
                           v2DocId: String,
                           v1Counts: Array[v1Count],
                           v21Counts: Array[v21Count],
                           v1Themes: Array[String],
                           v2Themes: Array[v2EnhancedTheme],
                           v1Locations: Array[v1Location],
                           v2Locations: Array[v2EnhancedLocation],
                           v1Persons: Array[String],
                           v2Persons: Array[v2EnhancedPerson],
                           v1Orgs: Array[String],
                           v2Orgs: Array[v2EnhancedOrg],
                           v1Stone: v1Stone,
                           v21Dates: V21EnhancedDate,
                           v2Gcam: Array[V2Gcam],
                           v21ShareImg: String,
                           v21RelImg: Array[String],
                           v21SocImg: Array[String],
                           v21SocVideo: Array[String],
                           v21Quotation: Array[V21Quotation],
                           v21AllNames: Array[V21Name],
                           v21Amounts: Array[V21Amount],
                           v21TransInfo: V21TranslationInfo,
                           v2ExtrasXml: String
                          )

  case class gkgRecordId(Date: Long, Translingual: Boolean, NumberInBatch: Int)
  case class v1Count(CountType: String, Count: Int, ObjectType: String, V1Location: v1Location)
  case class v1Location(LocationType: Int, FullName: String, CountryCode: String, ADM1Code: String, LocationLat: Float, LocationLong: Float, FeatureId: String)
  case class v21Count(v1Count: v1Count, charOffset: Int)
  case class v2EnhancedTheme(v1Theme: String, offset: Int)
  case class v2EnhancedLocation(v1Location: v1Location, adm2Code: String, charOffset: Int)
  case class v2EnhancedPerson(v1Person: String, charOffset: Int)
  case class v2EnhancedOrg(v1Org: String, charOffset: Int)
  case class v1Stone(tone: Float, positiveScore: Float, negativeScore: Float, polarity: Float, activityRefSens: Float, selfGroupRefDensity: Float, wordCount: Int)
  case class V21EnhancedDate(dateResolution: Int, month: Int, day: Int, year: Int, charOffset: Int)
  case class V2Gcam(dictionaryDimId: String, score: Float)
  case class V21Quotation(offset: Int, charLength: Int, verb: String, quote: String)
  case class V21Name(name: String, charOffset: Int)
  case class V21Amount(amount: Long, obj: String, offset: Int)
  case class V21TranslationInfo(srclc: String, eng: String)


  def createGkgRecordId(str: String): gkgRecordId = {
    if (str != "") {
      val split = str.split("-")
      if (split {
        1
      }.length > 1) {
        gkgRecordId(split {
          0
        }.toLong, true, split {
          1
        }.substring(1).toInt)
      }
      else {
        gkgRecordId(split {
          0
        }.toLong, false, split {
          1
        }.toInt)
      }
    }
    else {
      gkgRecordId(0L, false, 0)
    }
  }

  def createV1Counts(str: String) : Array[v1Count] = {
    val arrayBuffer = new ArrayBuffer[v1Count]
    str.split(";").foreach{
      f => val res = createV1Count(f)
        arrayBuffer += res
    }
    arrayBuffer.toArray
  }

  def createV1Count(str: String) : v1Count = {
    val blocks = str.split("#")
    val v1Location = createV1Location(str, 3)
    var ret = v1Count("",0,"",v1Location)
    if (blocks.length > 9) {
      ret = v1Count(blocks {
        0
      }, blocks {
        1
      }.toInt, blocks {
        2
      }, v1Location)
    }
    ret
  }

  def createV1Location(str: String, offset: Int) : v1Location = {
    var ret = v1Location(0,"","","",0.0f,0.0f,"")
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

      ret = v1Location(loctype, blocks {
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

  def createV21Counts(str: String) : Array[v21Count] = {
    val arrayBuffer = new ArrayBuffer[v21Count]
    str.split(";").foreach{
      f => if(f != ""){
        val v1Count = createV1Count(f)
        arrayBuffer += v21Count(v1Count, f.substring(f.lastIndexOf('#') + 1).toInt)
      }
    }
    arrayBuffer.toArray
  }

  def createV1Themes(str: String) : Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]
    str.split(";").foreach(f => arrayBuffer += f)
    arrayBuffer.toArray
  }

  def createV2EnhancedThemes(str: String) : Array[v2EnhancedTheme] = {
    val arrayBuffer = new ArrayBuffer[v2EnhancedTheme]
    str.split(";").foreach {
      f => if (f != "") {
        val s = f.split(",")
        arrayBuffer += v2EnhancedTheme(s {
          0
        }, s {
          1
        }.toInt)
      }
    }
    arrayBuffer.toArray
  }

  def createV1Locations(str: String) : Array[v1Location] = {
    val arrayBuffer = new ArrayBuffer[v1Location]
    str.split(";").foreach{
      f => val location = createV1Location(f, 0)
        arrayBuffer += location
    }
    arrayBuffer.toArray
  }

  def  createV2Locations(str: String) : Array[v2EnhancedLocation] = {
    val arrayBuffer = new ArrayBuffer[v2EnhancedLocation]
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
          arrayBuffer += v2EnhancedLocation(v1Location(blocks {
            0
          }.toInt, blocks {
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
          }.toInt)
        }
    }

    arrayBuffer.toArray
  }

  def createV1Persons(str: String) : Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]
    str.split(";").foreach(f => arrayBuffer += f)
    arrayBuffer.toArray
  }

  def createV2Persons(str: String): Array[v2EnhancedPerson] = {
    val arrayBuffer = new ArrayBuffer[v2EnhancedPerson]
    str.split(";").foreach(f => arrayBuffer += createEnhancedPerson(f))
    arrayBuffer.toArray
  }

  def createEnhancedPerson(str: String): v2EnhancedPerson = {
    val parts = str.split(",")
    var ret = v2EnhancedPerson("", 0)
    if (parts.length == 2) {
      ret = v2EnhancedPerson(parts {
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

  def createV2Orgs(str: String) : Array[v2EnhancedOrg] = {
    val arrayBuffer = new ArrayBuffer[v2EnhancedOrg]
    str.split(";").foreach(f => arrayBuffer += createEnhancedOrganisation(f))
    arrayBuffer.toArray
  }

  def createEnhancedOrganisation(str: String): v2EnhancedOrg = {
    var ret = v2EnhancedOrg("", 0)
    val split = str.split(",")
    if(split.length == 2){
      ret = v2EnhancedOrg(split{0}, split{1}.toInt)
    }
    ret
  }

  def createV1Stone(str: String): v1Stone = {
    var ret = v1Stone(0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0)
    if(str != ""){
      val split = str.split(",")
      ret = v1Stone(split{0}.toFloat,split{1}.toFloat,split{2}.toFloat,split{3}.toFloat,split{4}.toFloat,split{5}.toFloat,split{6}.toInt)
    }
    ret
  }

  def createV21Dates(str: String): Array[V21EnhancedDate] = {
    val arrayBuffer = new ArrayBuffer[V21EnhancedDate]
    str.split(";").foreach(f => arrayBuffer += createEnhancedDate(f))
    arrayBuffer.toArray
  }

  def createEnhancedDate(str: String): V21EnhancedDate = {
    var ret = V21EnhancedDate(0, 0, 0, 0, 0)
    val split = str.split(",")
    if(split.length == 5){
      ret = V21EnhancedDate(split{0}.toInt,split{1}.toInt,split{2}.toInt,split{3}.toInt,split{4}.toInt)
    }
    ret
  }

  def createV2GCAM(str: String): Array[V2Gcam] = {
    val arrayBuffer = new ArrayBuffer[V2Gcam]
    str.split(",").foreach(f => arrayBuffer += createGcam(f))
    arrayBuffer.toArray
  }

  def createGcam(str: String): V2Gcam = {
    var ret = V2Gcam("", 0.0f)
    val split = str.split(":")
    if(split.length == 2){
      ret = V2Gcam(split{0},split{1}.toFloat)
    }
    ret
  }

  def createV21RelImgAndVid(str: String): Array[String] = {
    val arrayBuffer = new ArrayBuffer[String]
    str.split(";").foreach(f => arrayBuffer += f)
    arrayBuffer.toArray
  }

  def createV21Quotations(str: String): Array[V21Quotation] = {
    val arrayBuffer = new ArrayBuffer[V21Quotation]
    str.split("#").foreach(f => arrayBuffer += createQuotation(f))
    arrayBuffer.toArray
  }

  def createQuotation(str: String): V21Quotation = {
    var ret = V21Quotation(0, 0, "", "")
    val split = str.split("\\|")
    if(split.length == 4){
      ret = V21Quotation(split{0}.toInt, split{1}.toInt, split{2}, split{3})
    }
    ret
  }

  def createV21AllNames(str: String): Array[V21Name] = {
    val arrayBuffer = new ArrayBuffer[V21Name]
    str.split(";").foreach(f => arrayBuffer += createName(f))
    arrayBuffer.toArray
  }

  def createName(str: String): V21Name = {
    var ret = V21Name("", 0)
    val split = str.split(",")
    if(split.length == 2){
      ret = V21Name(split{0}, split{1}.toInt)
    }
    ret
  }

  def createV21Amounts(str: String): Array[V21Amount] = {
    val arrayBuffer = new ArrayBuffer[V21Amount]
    str.split(";").foreach(f => arrayBuffer += createAmount(f))
    arrayBuffer.toArray
  }

  def createAmount(str: String): V21Amount = {
    var ret = V21Amount(0L, "", 0)
    val split = str.split(",")
    if(split.length == 3){
      ret = V21Amount(split{0}.toLong, split{1}, split{2}.toInt)
    }
    ret
  }

  def createV21TransInfo(str: String): V21TranslationInfo = {
    var ret = V21TranslationInfo("", "")
    val split = str.split(";")
    if(split.length == 2){
      ret = V21TranslationInfo(split{0}, split{1})
    }
    ret
  }
}