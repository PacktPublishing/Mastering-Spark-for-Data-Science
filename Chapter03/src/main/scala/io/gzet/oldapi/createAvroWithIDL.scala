package io.gzet.oldapi

import java.util

import org.io.gzet.gdelt.gkg.v1.{Location, Stone}
import org.io.gzet.gdelt.gkg.v2._
import org.io.gzet.gdelt.gkg.v21._

object CreateAvroWithIDL {

  def generateAvro(line: String): Specification = {

    val values = line.split("\t",-1)
    if(values.length == 27){
      val specification = Specification.newBuilder()
        .setGkgRecordId(createGkgRecordId(values{0}))
        .setV21Date(values{1}.toLong)
        .setV2SourceCollectionIdentifier(createSourceCollectionIdentifier(values{2}))
        .setV21SourceCommonName(values{3}).setV2DocumentIdentifier(values{4})
        .setV1Counts(createV1CountArray(values{5}))
        .setV21Counts(createV21CountArray(values{6}))
        .setV1Themes(createV1Themes(values{7}))
        .setV2EnhancedThemes(createV2EnhancedThemes(values{8}))
        .setV1Locations(createV1LocationsArray(values{9}))
        .setV2EnhancedLocations(createV2EnhancedLocations(values{10}))
        .setV1Persons(createV1PersonsArray(values{11}))
        .setV2EnhancedPersons(createV2EnhancedPersonsArray(values{12}))
        .setV1Organisations(createV1OrganisationsArray(values{13}))
        .setV2EnhancedOrganisations(createV2EnhancedOrganisationsArray(values{14}))
        .setV1Stone(createStone(values{15}))
        .setV21EnhancedDates(createEnhancedDatesArray(values{16}))
        .setV2Gcam(createGcamArray(values{17}))
        .setV21SharingImage(values{18})
        .setV21RelatedImages(createV21RelatedImagesArray(values{19}))
        .setV21SocialImageEmbeds(createSocialImageEmbedsArray(values{20}))
        .setV21SocialVideoEmbeds(createSocialVideoEmbeds(values{21}))
        .setV21Quotations(createV21QuotationsArray(values{22}))
        .setV21AllNames(createV21AllNamesArray(values{23}))
        .setV21Amounts(createV21AmountsArray(values{24}))
        .setV21TranslationInfo(createV21TranslationInfo(values{25}))
        .setV2ExtrasXML(values{26})

        specification.build
    }
    else {
      Specification.newBuilder.setGkgRecordId(createGkgRecordId("")).build
    }
  }

  def createGkgRecordId(str: String) : GkgRecordId = {
    val gkgRecordId = new GkgRecordId
    if(str != ""){
      val split = str.split("-")

      gkgRecordId.setDate(split{0}.toLong)

      if(split{1}.length > 1){
        gkgRecordId.setTranslingual(true)
        gkgRecordId.setNumberInBatch(split{1}.substring(1).toInt)
      }
      else {
          gkgRecordId.setTranslingual(false)
            gkgRecordId.setNumberInBatch(split{1}.toInt)}
    }
    gkgRecordId
  }

  def createSourceCollectionIdentifier(str: String) : SourceCollectionIdentifier = {
      str.toInt match {
      case 1 => SourceCollectionIdentifier.WEB
      case 2 => SourceCollectionIdentifier.CITATIONONLY
      case 3 => SourceCollectionIdentifier.CORE
      case 4 => SourceCollectionIdentifier.DTIC
      case 5 => SourceCollectionIdentifier.JSTOR
      case 6 => SourceCollectionIdentifier.NONTEXTUALSOURCE
      case _ => SourceCollectionIdentifier.WEB
    }
  }

  def createV1CountArray(str: String) : util.ArrayList[org.io.gzet.gdelt.gkg.v1.Count] = {
    val array = new util.ArrayList[org.io.gzet.gdelt.gkg.v1.Count]
    val counts = str.split(";")
    counts.foreach{
      f => val res = createV1Count(f)
        array.add(res)
    }
    array
  }

  def createV1Count(str: String) : org.io.gzet.gdelt.gkg.v1.Count = {
    val cnt = new org.io.gzet.gdelt.gkg.v1.Count
    val loc = new Location

    val blocks = str.split("#")
    if(blocks.length > 9) {
      cnt.setCountType(blocks {
        0
      })
      cnt.setCount(blocks {
        1
      }.toInt)
      cnt.setObjectType(blocks {
        2
      })

      loc.setLocationType(blocks {
        3
      }.toInt)
      loc.setFullName(blocks {
        4
      })
      loc.setCountryCode(blocks {
        5
      })
      loc.setADM1Code(blocks {
        6
      })
      if(blocks{7} != ""){
      loc.setLocationLatitude(blocks {
        7
      }.toFloat)}
      if(blocks{8} != ""){
      loc.setLocationLongitude(blocks {
        8
      }.toFloat)}
      loc.setFeatureId(blocks {
        9
      })
    }

    cnt.setV1Location(loc)
    cnt
  }

  def createV21CountArray(str: String) : util.ArrayList[org.io.gzet.gdelt.gkg.v21.Count] = {
    val array = new util.ArrayList[org.io.gzet.gdelt.gkg.v21.Count]
    str.split(";").foreach{
        f => if(f != ""){
          val v1Count = createV1Count(f)
          val v21Count = new org.io.gzet.gdelt.gkg.v21.Count
          v21Count.setV1Count(v1Count)
          v21Count.setCharOffset(f.substring(f.lastIndexOf('#') + 1).toInt)
          array.add(v21Count)
        }
    }
    array
  }

  def createV1Themes(str: String) : util.ArrayList[CharSequence] = {
    val array = new util.ArrayList[CharSequence]
    str.split(";").foreach(f => array.add(f))
    array
  }

  def createV2EnhancedThemes(str: String) : util.ArrayList[EnhancedTheme] = {
    val array = new util.ArrayList[EnhancedTheme]
    str.split(";").foreach {
      f => if(f != ""){
        val s = f.split(",")
        val enhancedTheme = new EnhancedTheme
        enhancedTheme.setTheme(s{0})
        enhancedTheme.setOffset(s{1}.toInt)
        array.add(enhancedTheme)}
    }
    array
  }

  def createV1LocationsArray(str: String) : util.ArrayList[Location] = {
    val array = new util.ArrayList[Location]
    val counts = str.split(";")
    counts.foreach{
      f => val location = createV1Location(f)
        array.add(location)
    }
    array
  }

  def createV1Location(str: String) : Location = {
    val loc = new Location

    val blocks = str.split("#")
    if(blocks.length == 7) {
      loc.setLocationType(blocks {
        0
      }.toInt)
      loc.setFullName(blocks {
        1
      })
      loc.setCountryCode(blocks {
        2
      })
      loc.setADM1Code(blocks {
        3
      })
      if(blocks{4} != ""){
        loc.setLocationLatitude(blocks {
          4
        }.toFloat)}
      if(blocks{5} != ""){
        loc.setLocationLongitude(blocks {
          5
        }.toFloat)}
      loc.setFeatureId(blocks {
        6
      })
    }
    loc
  }

  def  createV2EnhancedLocations(str: String) : util.ArrayList[EnhancedLocation] = {
    val enhancedLocationsArray = new util.ArrayList[EnhancedLocation]
    val allLocations = str.split(";")
    allLocations.foreach {
      f => val blocks = f.split("#")
        val loc = new Location
        val enhancedLoc = new EnhancedLocation
        if (blocks.length == 8) {
          loc.setLocationType(blocks {
            0
          }.toInt)
          loc.setFullName(blocks {
            1
          })
          loc.setCountryCode(blocks {
            2
          })
          loc.setADM1Code(blocks {
            3
          })
          if (blocks {
            5
          } != "") {
            loc.setLocationLatitude(blocks {
              5
            }.toFloat)
          }
          if (blocks {
            6
          } != "") {
            loc.setLocationLongitude(blocks {
              6
            }.toFloat)
          }
          loc.setFeatureId(blocks {
            7
          })
          enhancedLoc.setV1Location(loc)
          enhancedLoc.setADM2Code(blocks{4})
          enhancedLoc.setCharOffset(blocks{8}.toInt)
          enhancedLocationsArray.add(enhancedLoc)
        }
    }

    enhancedLocationsArray
  }

  def createV1PersonsArray(str: String) : util.ArrayList[CharSequence] = {
    val array = new util.ArrayList[CharSequence]
    str.split(";").foreach(f => array.add(f))
    array
  }

  def createV2EnhancedPersonsArray(str: String): util.ArrayList[EnhancedPerson] = {
    val array = new util.ArrayList[EnhancedPerson]
    str.split(";").foreach(f => array.add(createEnhancedPerson(f)))
    array
  }

  def createEnhancedPerson(str: String): EnhancedPerson = {
    val enhancedPerson = new EnhancedPerson
    val parts = str.split(",")
    if(parts.length == 2){
    enhancedPerson.setPerson(parts{0})
    enhancedPerson.setCharOffset(parts{1}.toInt)}
    enhancedPerson
  }

  def createV1OrganisationsArray(str: String): util.ArrayList[CharSequence] = {
    val array = new util.ArrayList[CharSequence]
    str.split(";").foreach(f => array.add(f))
    array
  }

  def createV2EnhancedOrganisationsArray(str: String) : util.ArrayList[EnhancedOrganisation] = {
    val array = new util.ArrayList[EnhancedOrganisation]
    str.split(";").foreach(f => array.add(createEnhancedOrganisation(f)))
    array
  }

  def createEnhancedOrganisation(str: String): EnhancedOrganisation = {
    val org = new EnhancedOrganisation
    val split = str.split(",")
    if(split.length == 2){
    org.setOrganisation(split{0})
    org.setCharOffset(split{1}.toInt)}
    org
  }

  def createStone(str: String): Stone = {
    val stone = new Stone
    if(str != ""){
      val split = str.split(",")
      stone.setTone(split{0}.toFloat)
      stone.setPositiveScore(split{1}.toFloat)
      stone.setNegativeScore(split{2}.toFloat)
      stone.setPolarity(split{3}.toFloat)
      stone.setActivityReferenceDensity(split{4}.toFloat)
      stone.setSelfGroupReferenceDensity(split{5}.toFloat)
      stone.setWordCount(split{6}.toInt)
    }
    stone
  }

  def createEnhancedDatesArray(str: String): util.ArrayList[EnhancedDate] = {
    val array = new util.ArrayList[EnhancedDate]
    str.split(";").foreach(f => array.add(createEnhancedDate(f)))
    array
  }

  def createEnhancedDate(str: String): EnhancedDate = {
    val enhancedDate = new EnhancedDate
    val split = str.split(",")
    if(split.length == 5){
      enhancedDate.setDateResolution(split{0}.toInt)
      enhancedDate.setMonth(split{1}.toInt)
      enhancedDate.setDay(split{2}.toInt)
      enhancedDate.setYear(split{3}.toInt)
      enhancedDate.setCharOffset(split{4}.toInt)
    }
    enhancedDate
  }

  def createGcamArray(str: String): util.ArrayList[Gcam] = {
    val array = new util.ArrayList[Gcam]
    str.split(",").foreach(f => array.add(createGcam(f)))
    array
  }

  def createGcam(str: String): Gcam = {
    val gcam = new Gcam
    val split = str.split(":")
    if(split.length == 2){
      gcam.setDictionaryDimensionID(split{1})
      gcam.setScore(split{1}.toFloat)
    }
    gcam
  }

  def createV21RelatedImagesArray(str: String): util.ArrayList[CharSequence] = {
    val array = new util.ArrayList[CharSequence]
    str.split(";").foreach(f => array.add(f))
    array
  }

  def   createSocialImageEmbedsArray(str: String): util.ArrayList[CharSequence] = {
    val array = new util.ArrayList[CharSequence]
    str.split(";").foreach(f => array.add(f))
    array
  }

  def   createSocialVideoEmbeds(str: String): util.ArrayList[CharSequence] = {
    val array = new util.ArrayList[CharSequence]
    str.split(";").foreach(f => array.add(f))
    array
  }

  def createV21QuotationsArray(str: String): util.ArrayList[Quotation] = {
    val array = new util.ArrayList[Quotation]
    str.split("#").foreach(f => array.add(createQuotation(f)))
    array
  }

  def createQuotation(str: String): Quotation = {
    val quotation = new Quotation
    val split = str.split("\\|")
    if(split.length == 4){
      quotation.setOffset(split{0}.toInt)
      quotation.setCharLength(split{1}.toInt)
      quotation.setVerb(split{2})
      quotation.setQuote(split{3})
    }
    quotation
  }

  def createV21AllNamesArray(str: String): util.ArrayList[Name] = {
    val array = new util.ArrayList[Name]
    str.split(";").foreach(f => array.add(createName(f)))
    array
  }

  def createName(str: String): Name = {
    val name = new Name
    val split = str.split(",")
    if(split.length == 2){
      name.setName(split{0})
      name.setCharOffset(split{1}.toInt)
    }
    name
  }

  def createV21AmountsArray(str: String): util.ArrayList[Amount] = {
    val array = new util.ArrayList[Amount]
    str.split(";").foreach(f => array.add(createAmount(f)))
    array
  }

  def createAmount(str: String): Amount = {
    val amount = new Amount
    val split = str.split(",")
    if(split.length == 3){
      amount.setAmount(split{0}.toLong)
      amount.setObject(split{1})
      amount.setOffset(split{2}.toInt)
    }
    amount
  }

  def createV21TranslationInfo(str: String): TranslationInfo = {
    val translationInfo = new TranslationInfo
    val split = str.split(";")
    if(split.length == 2){
      translationInfo.setSRCLC(split{0})
      translationInfo.setENG(split{1})
    }
    translationInfo
  }
}

