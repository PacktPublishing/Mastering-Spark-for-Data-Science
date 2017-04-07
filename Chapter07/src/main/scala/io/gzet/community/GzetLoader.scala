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

package io.gzet.community

import com.typesafe.config.ConfigFactory
import io.gzet.community.accumulo.{AccumuloLoader, AccumuloConfig}
import io.gzet.community.elasticsearch.{ESReader, ESConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GzetLoader {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .appName("communities-loader")
      .getOrCreate()

    val sc = spark.sparkContext

    val blacklist = args.mkString(" ").split(",").map(_.trim).toSet
    val config = ConfigFactory.load()

    val esField = config.getString("io.gzet.elasticsearch.field")
    val esConf = ESConfig(
      config.getString("io.gzet.elasticsearch.nodes"),
      config.getInt("io.gzet.elasticsearch.port"),
      config.getString("io.gzet.elasticsearch.index")
    )

    val accumuloTable = config.getString("io.gzet.accumulo.table")
    val accumuloConf = AccumuloConfig(
      config.getString("io.gzet.accumulo.instance"),
      config.getString("io.gzet.accumulo.user"),
      config.getString("io.gzet.accumulo.password"),
      config.getString("io.gzet.accumulo.zookeeper")
    )

    val reader = new ESReader(esConf)
    val personsRdd = reader.loadPersons(sc, esField)
    personsRdd.cache()

    val writer = new AccumuloLoader(accumuloConf)
    writer.persist(sc, accumuloTable, personsRdd, blacklist)

  }

}
