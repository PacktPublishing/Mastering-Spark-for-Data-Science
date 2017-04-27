package io.gzet.profilers

import io.gzet.profilers.field.{CardinalityProfiler, EmptinessProfiler, MaskBasedProfiler, PredefinedMasks}
import io.gzet.profilers.raw.{AsciiProfiler, RowProfiler, StructuralProfiler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.sql._

object GdeltStructuralProfiler {

  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  val HEADER = Array(
    "cameo",
    "day",
    "month",
    "year",
    "fracDate",
    "actor1Code",
    "actor1Name",
    "actor1CountryCode",
    "actor1KnownGroupCode",
    "actor1EthnicCode",
    "actor1Religion1Code",
    "actor1Religion2Code",
    "actor1Type1Code",
    "actor1Type2Code",
    "actor1Type3Code",
    "actor2Code",
    "actor2Name",
    "actor2CountryCode",
    "actor2KnownGroupCode",
    "actor2EthnicCode",
    "actor2Religion1Code",
    "actor2Religion2Code",
    "actor2Type1Code",
    "actor2Type2Code",
    "actor2Type3Code",
    "isRoot",
    "eventCode",
    "eventBaseCode",
    "eventRootCode",
    "quadClass",
    "goldstein",
    "numMentions",
    "numSources",
    "numArticles",
    "avgTone",
    "actor1GeoType",
    "actor1GeoName",
    "actor1GeoCountryCode",
    "actor1GeoAdm1Code",
    "actor1GeoAdm2Code",
    "actor1GeoLatitude",
    "actor1GeoLongitude",
    "actor1GeoFeatureId",
    "actor2GeoType",
    "actor2GeoName",
    "actor2GeoCountryCode",
    "actor2GeoAdm1Code",
    "actor2GeoAdm2Code",
    "actor2GeoLatitude",
    "actor2GeoLongitude",
    "actor2GeoFeatureId",
    "eventGeoType",
    "eventGeoName",
    "eventGeoCountryCode",
    "eventGeoAdm1Code",
    "eventGeoAdm2Code",
    "eventGeoLatitude",
    "eventGeoLongitude",
    "eventGeoFeatureId",
    "dateAdded",
    "sourceUrl"
  )

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Profiler").getOrCreate()
    import spark.implicits._

    val rawDf: Dataset[String] = spark.read.text(args.head).map(_.getAs[String](0)).repartition(4)
    rawDf.cache()
    rawDf.count()

    val tabDf: Dataset[Array[String]] = Utils.split(rawDf, delimiter = "\t")

    val sources = spark.sparkContext.broadcast(rawDf.inputFiles)
    val ingestTime = spark.sparkContext.broadcast(new java.util.Date().getTime)

    val headers = spark.sparkContext.broadcast(HEADER.zipWithIndex.map(_.swap).toMap)

    RowProfiler.apply().profile(rawDf).map({ report =>
      ("row.count", report.metricValue, Map[String, String]())
    }).union(AsciiProfiler.apply().profile(rawDf).map({ report =>
      ("row.ascii", report.metricValue, Map(Tags.ASCII_NAME -> report.ascii, Tags.ASCII_BINARY -> report.binary))
    })).union(StructuralProfiler.apply(delimiter = "\t").profile(rawDf).map({ report =>
      ("field.count", report.metricValue, Map(Tags.EXTRA -> report.description, Tags.FIELD_COUNT -> report.fields.toString))
    })).union(EmptinessProfiler.apply().profile(tabDf).map({ report =>
      ("field.emptiness", report.metricValue, Map(Tags.FIELD_IDX -> report.field.toString))
    })).union(CardinalityProfiler.apply(topN = 5).profile(tabDf).map({ report =>
      ("field.cardinality", report.metricValue, Map(Tags.FIELD_IDX -> report.field.toString, Tags.EXTRA -> report.description.map(l => s"[$l]").mkString(",")))
    })).union(MaskBasedProfiler.apply(topN = 5, PredefinedMasks.ASCIICLASS_LOWGRAIN).profile(tabDf).map({ report =>
      ("field.ascii", report.metricValue, Map(Tags.FIELD_IDX -> report.field.toString, Tags.MASK -> report.mask, Tags.EXTRA -> report.description.map(l => s"[$l]").mkString(",")))
    })).union(MaskBasedProfiler.apply(topN = 5, PredefinedMasks.POP_CHECKS).profile(tabDf).map({ report =>
      ("field.pop.check", report.metricValue, Map(Tags.FIELD_IDX -> report.field.toString, Tags.MASK -> report.mask, Tags.EXTRA -> report.description.map(l => s"[$l]").mkString(",")))
    })).union(MaskBasedProfiler.apply(topN = 5, PredefinedMasks.CLASS_FREQS).profile(tabDf).map({ report =>
      ("field.class.freq", report.metricValue, Map(Tags.FIELD_IDX -> report.field.toString, Tags.MASK -> report.mask, Tags.EXTRA -> report.description.map(l => s"[$l]").mkString(",")))
    })).map({ case (metricName, metricValue, tags) =>
      val newTags = {
        if (tags.contains(Tags.FIELD_IDX)) {
          val fieldIdx = tags.get(Tags.FIELD_IDX).get.toInt
          val fieldName = headers.value.getOrElse(fieldIdx, "NA")
          tags ++ Map(Tags.FIELD_NAME -> fieldName)
        } else {
          tags
        }
      }

      ReportBuilder.create
        .withName(metricName)
        .withMetric(metricValue)
        .withSources(sources.value)
        .withTime(ingestTime.value)
        .withTags(newTags)
        .build

    }).toDF().saveToEs("profiler/gdelt")

  }

}