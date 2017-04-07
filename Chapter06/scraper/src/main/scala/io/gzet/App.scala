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

import java.text.SimpleDateFormat

import com.typesafe.config.ConfigFactory
import io.gzet.GeoLookup.{GeoName, GeoPoint, GeoPlace}
import io.gzet.HtmlFetcher.Content
import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator, Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.elasticsearch.spark._
import org.slf4j.LoggerFactory

object App {

  final val ISO_SDF = "yyyy-MM-dd'T'HH:mm:ssZ"
  final val GDELT_SDF = "yyyyMMddHHmmss"
  final val sdfIn = new SimpleDateFormat(GDELT_SDF)
  final val sdfOut = new SimpleDateFormat(ISO_SDF)
  final val logger = LoggerFactory.getLogger(getClass)

  case class Article(
                      publishedDate: String,
                      url: String,
                      title: Option[String],
                      description: Option[String],
                      body: Option[String],
                      persons: Array[String],
                      organizations: Array[String],
                      geoPoints: Array[GeoPoint],
                      places: Array[String],
                      countries: Array[String],
                      states: Array[String],
                      locations: Array[GeoPlace]
                    )

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val config = ConfigFactory.load()

    val esEnabled = config.getBoolean("gzet.es.enable")
    val esNodes = config.getString("gzet.es.nodes")
    val esResource = config.getString("gzet.es.resource")
    val esPort = config.getInt("gzet.es.port")
    val gdeltInputDir = config.getString("gzet.gdelt.input.hdfs")
    val gdeltOutputDir = config.getString("gzet.gdelt.output.hdfs")
    val geonamePath = config.getString("gzet.geoname.input.hdfs")
    val htmlPartitions = config.getInt("gzet.gdelt.html.partitions")

    val spark = SparkSession
      .builder()
      .appName("Web scraping")
      .config("es.index.auto.create", "true")
      .config("es.nodes", esNodes)
      .config("es.port", esPort.toString)
      .getOrCreate()

    val sc = spark.sparkContext
    val files = readHdfs(sc, gdeltInputDir)

    logger.info(s"Loading GeoNames")
    val geonameRdd = sc.objectFile[GeoName](geonamePath).persist(StorageLevel.DISK_ONLY)

    for((time, file) <- files) {
      val date = sdfOut.format(sdfIn.parse(time))
      logger.info(s"*******************************************")
      logger.info(s"Processing GDELT [$date]")
      logger.info(s"*******************************************")
      process(sc, date, time, file, geonameRdd, gdeltOutputDir, htmlPartitions, esEnabled, esResource)
      tearDown(sc, file)
    }
  }

  def process(sc: SparkContext, date: String, time: String, file: String, geonameRdd: RDD[GeoName], outputDir: String, partitions: Int, esEnabled: Boolean, esResource: String) = {

    // Get URLs from GDELT records
    logger.info("Extracting URLs from GDELT data")
    val urlRdd = sc.textFile(file).map(_.split("\\t").last).filter(_.startsWith("http")).distinct().zipWithIndex().map(_.swap).repartition(partitions).cache()

    // Retrieve HTML content
    logger.info(s"Fetching HTML content from ${urlRdd.count()} URLs")
    val articleRdd: RDD[(Long, Content)] = fetchHtml(urlRdd).persist(StorageLevel.DISK_ONLY)
    articleRdd.count()

    // Extract all NLP entities
    val corpusRdd: RDD[(Long, String)] = articleRdd.filter(_._2.body.isDefined) map {case (id, content) => (id, content.body.get) } cache()
    logger.info(s"Extracting NLP entities from ${corpusRdd.count()} articles")
    val nlpRdd: RDD[(Long, Entities)] = extractEntities(corpusRdd).persist(StorageLevel.DISK_ONLY)
    nlpRdd.count()
    corpusRdd.unpersist(blocking = false)

    // Retrieve locations
    val nlpLocationRdd = nlpRdd flatMap { case (id, nlp) => nlp.getEntities("LOCATION").distinct.map(entity => (id, entity))} cache()
    logger.info(s"Geo locating ${nlpLocationRdd.values.distinct().count} distinct locations")
    val locationRdd: RDD[(Long, Set[GeoPlace])] = getLocations(sc, nlpLocationRdd, geonameRdd).persist(StorageLevel.DISK_ONLY)
    locationRdd.count()
    nlpLocationRdd.unpersist(blocking = false)

    // Retrieve persons
    val nlpPersonRdd = nlpRdd flatMap { case (id, nlp) => nlp.getEntities("PERSON").distinct.map(entity => (id, entity))} cache()
    logger.info(s"De-duplicating ${nlpPersonRdd.values.distinct().count} distinct names")
    val personRdd: RDD[(Long, Set[String])] = getPersons(nlpPersonRdd).persist(StorageLevel.DISK_ONLY)
    personRdd.count()
    nlpPersonRdd.unpersist(blocking = false)

    // Retrieve organizations
    val nlpOrganizationRdd = nlpRdd flatMap { case (id, nlp) => nlp.getEntities("ORGANIZATION").distinct.map(entity => (id, entity))} cache()
    logger.info(s"De-duplicating ${nlpOrganizationRdd.values.distinct().count} distinct organizations")
    val organizationRdd: RDD[(Long, Set[String])] = getOrganizations(nlpOrganizationRdd).persist(StorageLevel.DISK_ONLY)
    organizationRdd.count()
    nlpOrganizationRdd.unpersist(blocking = false)

    // Merge all the result from our generic classes into a single object
    logger.info("Merging results into Article case class")
    val finalRdd = mergeResults(articleRdd, nlpRdd, locationRdd, personRdd, organizationRdd, date).cache()

    // Output to Elasticsearch (if enabled)
    if(esEnabled) {
      logger.info(s"Publishing ${finalRdd.count()} articles to elasticsearch")
      finalRdd.saveToEs(esResource)
    }

    // Also output the JSON copy on DISK
    logger.info(s"Saving ${finalRdd.count()} articles as Json")
    toJson(finalRdd).saveAsTextFile(s"$outputDir/$time")

    // Make sure you unpersist everything before calling the next file
    finalRdd.unpersist(blocking = false)
    organizationRdd.unpersist(blocking = false)
    personRdd.unpersist(blocking = false)
    locationRdd.unpersist(blocking = false)
    nlpRdd.unpersist(blocking = false)
    articleRdd.unpersist(blocking = false)

  }

  def fetchHtml(urlRdd: RDD[(Long, String)]) = {
    new HtmlFetcher().fetchWithContext(urlRdd)
  }

  def extractEntities(corpusRdd: RDD[(Long, String)]) = {
    new EntityRecognition().extractWithContext(corpusRdd)
  }

  def getLocations(sc: SparkContext, locationRdd: RDD[(Long, String)], geonameRdd: RDD[GeoName]) = {
    new GeoLookup().locateBatchWithContext(sc, locationRdd, geonameRdd).groupByKey().mapValues(_.toSet[GeoPlace])
  }

  def getPersons(personRdd: RDD[(Long, String)]) = {
    new NameDeduplication().deduplicateWithContext(personRdd).groupByKey().mapValues(_.toSet[String])
  }

  def getOrganizations(organizationRdd: RDD[(Long, String)]) = {
    new OrganizationDeduplication().deduplicateWithContext(organizationRdd).groupByKey().mapValues(_.toSet[String])
  }

  def mergeResults(contentRdd: RDD[(Long, Content)], nlpRdd: RDD[(Long, Entities)], locationRdd: RDD[(Long, Set[GeoPlace])], personRdd: RDD[(Long, Set[String])], organizationRdd: RDD[(Long, Set[String])], defaultDate: String) = {

    contentRdd.filter(_._2.body.isDefined).join(nlpRdd).leftOuterJoin(locationRdd).map({case (id, ((content, nlp), optLocation)) =>
      (id, (content, nlp, optLocation.getOrElse(Set()).toArray))
    }).leftOuterJoin(personRdd).map({ case (id, ((content, nlp, locations), optPersons)) =>
      (id, (content, nlp, locations, optPersons.getOrElse(Set()).toArray))
    }).leftOuterJoin(organizationRdd).map({ case (id, ((content, nlp, locations, persons), optOrgs)) =>
      (id, (content, nlp, locations, persons, optOrgs.getOrElse(Set()).toArray))
    }).map({case (id, (content, nlp, locations, persons, organizations)) =>
      val geoPoints = locations.map(_.coordinates)
      val countries = locations.map(_.country.getOrElse("")).filter(_.length > 0)
      val places = locations.map(_.name)
      val states = locations.map(_.state.getOrElse("")).filter(_.length > 0)
      Article(
        defaultDate,
        content.url,
        content.title,
        content.description,
        content.body,
        persons,
        organizations,
        geoPoints,
        places,
        countries,
        states,
        locations
      )
    })
  }

  def toJson(finalRdd: RDD[Article]) = {
    finalRdd map { case article =>
      implicit val formats = DefaultFormats
      write(article)
    }
  }

  def tearDown(sc: SparkContext, path: String) = {
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    hdfs.delete(new Path(path), false)
    hdfs.close()
  }

  def readHdfs(sc: SparkContext, path: String) = {
    val pattern = ".*\\/([0-9]{14})\\.export\\.CSV".r
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val it: RemoteIterator[LocatedFileStatus] = hdfs.listFiles(new Path(path), false)
    val files = collection.mutable.Map[String, String]()
    while(it.hasNext) {
      val file = it.next().getPath.toString
      val pattern(part) = file
      files.put(part, file)
    }
    hdfs.close()
    files.toMap
  }
}
