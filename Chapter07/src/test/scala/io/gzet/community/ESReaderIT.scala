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

import io.gzet.community.elasticsearch.{ESReader, ESConfig}
import io.gzet.test.SparkFunSuite
import org.apache.log4j.{Level, Logger}

class ESReaderIT extends SparkFunSuite {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  localTest("Read from ES") { spark =>

    val sc = spark.sparkContext
    val esConf = ESConfig("localhost", 9200, "gzet/articles")
    val esField = "persons"

    val reader = new ESReader(esConf)
    val esQuery = "?q=persons:'David Bowie'"
    val tuples = reader.loadPersons(sc, esField, esQuery)
    tuples.cache
    assert(tuples.count() > 0L)
    tuples.take(100).foreach(println)
  }
}
