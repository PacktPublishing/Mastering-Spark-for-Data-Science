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

package io.gzet

import breeze.util.BloomFilter
import com.neovisionaries.i18n.CountryCode
import io.gzet.GeoLookup.{GeoPlace, FeatureType, GeoName}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class GeoLookup() extends Serializable {

  def locate(rdd: RDD[String], geonames: RDD[(String, GeoName)]): RDD[(String, GeoPlace)] = {
    rdd map { location =>
      (GeoLookup.clean(location), location)
    } join geonames map { case (clean, (name, geoname)) =>
      (name, geoname)
    } groupByKey() mapValues { it =>
      dedup(it.toSet[GeoName])
    }
  }

  def locateWithContext(rdd: RDD[(Long, String)], geonames: RDD[(String, GeoName)]): RDD[(Long, GeoPlace)] = {
    val contextLocationRdd = rdd map { case (context, location) =>
      (GeoLookup.clean(location), context)
    } join geonames map { case (clean, (context, geoname)) =>
      ((context, clean), geoname)
    } groupByKey() map { case ((context, clean), it) =>
      (context, it.toSet)
    }

    dedupWithContext(contextLocationRdd)
  }

  def locateBatch(sc: SparkContext, rdd: RDD[String], geonames:  RDD[GeoName]): RDD[(String, GeoPlace)] = {
    val bloomFilterSize = rdd.count()
    val bloomFilter = sc.broadcast(rdd mapPartitions { it =>
      val bf = BloomFilter.optimallySized[String](bloomFilterSize, 0.001)
      it foreach { name =>
        bf += GeoLookup.clean(name)
      }
      Iterator(bf)
    } reduce(_ | _))

    val filteredGeonames = geonames filter { geoname =>
      geoname.names.count(bloomFilter.value.contains) > 0
    } flatMap { geoname =>
      geoname.names.map(name => (GeoLookup.clean(name), geoname))
    } filter { case (name, geoname) =>
      StringUtils.isNotEmpty(name)
    }

    locate(rdd, filteredGeonames)
  }

  def locateBatchWithContext(sc: SparkContext, rdd: RDD[(Long, String)], geonames:  RDD[GeoName]): RDD[(Long, GeoPlace)] = {
    val bloomFilterSize = rdd.count()
    val bloomFilter = sc.broadcast(rdd mapPartitions { it =>
      val bf = BloomFilter.optimallySized[String](bloomFilterSize, 0.001)
      it foreach { case (context, name) =>
        bf += GeoLookup.clean(name)
      }
      Iterator(bf)
    } reduce(_ | _))

    val filteredGeonames = geonames filter { geoname =>
      geoname.names.count(bloomFilter.value.contains) > 0
    } flatMap { geoname =>
      geoname.names.map(name => (GeoLookup.clean(name), geoname))
    } filter { case (name, geoname) =>
      StringUtils.isNotEmpty(name)
    }

    locateWithContext(rdd, filteredGeonames)
  }

  private def dedup(geonames: Set[GeoName]): GeoPlace = {

    val ranked = geonames map { geoname =>
      val rank = rankGeoName(geoname)
      (rank, geoname)
    } toList

    val sorted = ranked.sortBy { case (rank, geoname) =>
      (rank, 1.0d / (geoname.population + 1))
    }

    val best = sorted.head._2
    GeoPlace(best.name, best.country, best.adminCode, best.geoPoint)

  }

  private def dedupWithContext(geoNameRdd: RDD[(Long, Set[GeoName])]): RDD[(Long, GeoPlace)] = {

    // Retrieve all location candidates for a same context Id
    val dedupRdd = geoNameRdd.groupByKey().map({case (contextId, locations) =>

      val locationsList = locations.toList

      // Find all the distinct countries / states for each location
      val countryZones = collection.mutable.Map[String, Set[Long]]()
      val stateZones = collection.mutable.Map[String, Set[Long]]()

      locationsList.foreach({candidates =>
        candidates.foreach({candidate =>

          if(isCountry(candidate)) {
            // If one of the candidate is a country, add both its timezone head (Europe/London => Europe) and Country Name
            // This helps getting Belfast in Northern Ireland instead of Ireland...
            // Also add its ID to the set who contributed to that country
            val countries = Array(candidate.timezone.head, candidate.country.getOrElse(""))
            countries.filter(_.length > 0).foreach({country =>
              countryZones.put(country, countryZones.getOrElse(country, Set()) ++ Set(candidate.geoId))
            })
          } else if(isState(candidate)) {
            // If one of the candidate is a state (ADM1 only), add its State Name
            // Also add its ID to the set who contributed to that state
            val states = Array(candidate.adminCode.getOrElse(""))
            states.filter(_.length > 0).foreach({state =>
              stateZones.put(state, stateZones.getOrElse(state, Set()) ++ Set(candidate.geoId))
            })
          }
        })
      })

      // ASSUMPTION
      // **********
      // We may have several locations mentioned in this document, with potentially many candidates
      // We get all countries and states already mentioned in this document
      // If a document is only focused on Canada and nothing is mentioning the United Kingdom,
      // then London is most likely the place in Canada rather than in the United Kingdom
      // If no country / state is mentioned, or if both Canada and United Kingdom are found,
      // then we use the most important London from our matching dataset (i.e. UK)
      // Also sometimes people say Ireland in lieu of Northern Ireland, thus making Belfast to fail!
      // We retrieve the timezone Europe/Ireland for that purpose.

      val bestLocationCandidates = locationsList.map({ locationCandidates =>

        // We sort our list based on "isTheCountry", "isTheState" mentioned?
        // Then what rank is it? State / County is higher than a lake with a same name
        // And then sort with population
        // Return the element from the top of this list.. And pray!
        val bestCandidates = locationCandidates.map({candidate =>
          val rank = rankGeoName(candidate)
          val countryZone = Array(candidate.timezone.head, candidate.country.getOrElse("")).filter(_.length > 0)
          val stateZone = Array(candidate.adminCode.getOrElse("")).filter(_.length > 0)
          val isCountryZone = countryZone.exists(countryZones.contains)
          val isStateZone = stateZone.exists({ zone =>
            stateZones.getOrElse(zone, Set(candidate.geoId)).diff(Set(candidate.geoId)).nonEmpty
          })

          (candidate, rank, isCountryZone, isStateZone)
        }).toList.sortBy({case (candidate, rank, isCountry, isState) =>
          (isCountry, isState, 1 / (rank.toDouble + 1), candidate.population)
        }).reverse
        bestCandidates.head._1
      })

      (contextId, bestLocationCandidates)

    })

    dedupRdd.flatMap({case (documentId, geonames) =>
      geonames map { geoname =>
        val place = GeoPlace(geoname.name, geoname.country, geoname.adminCode, geoname.geoPoint)
        (documentId, place)
      }
    })
  }

  private def isCountry(geoName: GeoName) = {
    GeoLookup.geoRank.getOrElse(geoName.featureCode, (FeatureType.Other, Int.MaxValue))._1 == FeatureType.Country
  }

  private def isState(geoName: GeoName) = {
    GeoLookup.geoRank.getOrElse(geoName.featureCode, (FeatureType.Other, Int.MaxValue))._1 == FeatureType.State
  }

  private def rankGeoName(geoName: GeoName): Int = {
    GeoLookup.geoRank.getOrElse(geoName.featureCode, (FeatureType.Other, Int.MaxValue))._2
  }

}

object GeoLookup {

  case class GeoName(
                      geoId: Long,
                      name: String,
                      names: Set[String],
                      country: Option[String],
                      adminCode: Option[String],
                      featureCode: String,
                      population: Long,
                      timezone: Array[String],
                      geoPoint: GeoPoint
                    )

  case class GeoPoint(
                       lat: Double,
                       lon: Double
                     )

  case class GeoPlace(
                       name: String,
                       country: Option[String],
                       state: Option[String],
                       coordinates: GeoPoint
                     )

  def clean(name: String): String = {
    StringUtils.stripAccents(name.toLowerCase())
      .replaceAll("\\.", "")
      .replaceAll("\\W+", " ")
      .split("[^a-z]")
      .map(_.trim)
      .mkString(" ")
  }

  def load(sc: SparkContext, adminCodesPath: String, allCountriesPath: String): RDD[GeoName] = {

    val adminCodeMap = sc.textFile(adminCodesPath) map { line =>
      val a = line.split("\\t")
      (a(0), a(1))
    } collectAsMap

    sc.textFile(allCountriesPath) map { line =>

      val a = line.split("\\t")

      // Unique reference ID
      val geoId = a(0).toLong

      // Place ASCII name and alternative names
      val name = a(2)
      val altNames = ((Array(a(1), a(2)) ++ a(3).split(",")) map clean filter(_.length > 0)).toSet

      // Geo coordinates
      val lat = a(4).toDouble
      val lng = a(5).toDouble
      val geoPoint = GeoPoint(lat, lng)

      // Place type
      val fCode = a(7)

      // Country / County
      val countryCode = a(8)
      val adminCode = adminCodeMap.get(s"$countryCode.${a(10)}")
      val country = {
        val cc = CountryCode.getByCode(a(8).toUpperCase())
        if(cc != null) {
          Some(cc.getName)
        } else {
          None: Option[String]
        }
      }

      // Population
      val population = a(14).toLong

      // Timezone
      val timezone = if(a(17).length > 0) {
        a(17).split("/").take(2)
      } else {
        Array.fill[String](2)("")
      }

      GeoName(
        geoId,
        name,
        altNames,
        country,
        adminCode,
        fCode,
        population,
        timezone,
        geoPoint
      )

    } filter { geoname =>
      geoRank.getOrElse(geoname.featureCode, FeatureType.NA) != FeatureType.NA
    }
  }

  object FeatureType extends Enumeration {
    type FeatureType = Value
    val Zone, Country, State, MajorCity, Other, NA = Value
  }

  val geoRank = Map(
    "CONT" -> (FeatureType.Zone, 0),
    "ZN" -> (FeatureType.Zone, 1),
    "PCL" -> (FeatureType.Country, 2),
    "PCLD" -> (FeatureType.Country, 2),
    "PCLF" -> (FeatureType.Country, 2),
    "PCLH" -> (FeatureType.Country, 2),
    "PCLI" -> (FeatureType.Country, 2),
    "PPLC" -> (FeatureType.MajorCity, 2),
    "PPLG" -> (FeatureType.MajorCity, 3),
    "PPLA" -> (FeatureType.MajorCity, 3),
    "PPLA2" -> (FeatureType.MajorCity, 3),
    "ADM1" -> (FeatureType.State, 3),
    "ADM2" -> (FeatureType.Other, 4),
    "ADM3" -> (FeatureType.Other, 6),
    "PPLA3" -> (FeatureType.Other, 7),
    "ADM4" -> (FeatureType.Other, 8),
    "ADM5" -> (FeatureType.Other, 8),
    "SEA" -> (FeatureType.Other, 8),
    "MNT" -> (FeatureType.Other, 8),
    "MNTS" -> (FeatureType.Other, 8),
    "STM" -> (FeatureType.Other, 8),
    "CNYN" -> (FeatureType.Other, 8),
    "PPLA4" -> (FeatureType.Other, 10),
    "PPL" -> (FeatureType.Other, 12)
  )

}