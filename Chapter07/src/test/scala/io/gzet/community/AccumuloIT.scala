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

import io.gzet.community.accumulo.{AccumuloAuthorization, AccumuloReader, AccumuloLoader, AccumuloConfig}
import io.gzet.test.SparkFunSuite
import org.apache.log4j.{Level, Logger}

class AccumuloIT extends SparkFunSuite {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  localTest("Write to Accumulo, Read from Accumulo") { spark =>

    val sc = spark.sparkContext
    val accumuloConf = AccumuloConfig("ACCUMULO_INSTANCE", "root", "secret", "localhost:2181")
    val accumuloTable = "persons"

    val writer = new AccumuloLoader(accumuloConf)
    val persisted = sc.parallelize(Seq(("Antoine Amend", "Matthew Hallett")))
    writer.persist(sc, accumuloTable, persisted)

    val reader = new AccumuloReader(accumuloConf)
    val retrieved = reader.read(sc, accumuloTable)
    retrieved.cache()

    val filtered = retrieved.filter(_.getSourceVertex == "Antoine Amend")
    filtered.cache()
    filtered.count should be(1L)
    filtered.map(_.getDestVertex).first() should be("Matthew Hallett")
    filtered.map(_.getCount).first() should be(1L)
    filtered.map(_.toString).take(1).foreach(println)

    writer.persist(sc, accumuloTable, persisted)
    val retrieved2 = reader.read(sc, accumuloTable)
    val filtered2 = retrieved2.filter(_.getSourceVertex == "Antoine Amend")

    filtered2.cache()
    filtered2.count should be(1L)
    filtered2.map(_.getDestVertex).first() should be("Matthew Hallett")
    filtered2.map(_.getCount).first() should be(2L)
    filtered2.map(_.toString).take(1).foreach(println)
  }


  localTest("Row security") { spark =>

    val accumuloConf = AccumuloConfig("ACCUMULO_INSTANCE", "root", "secret", "localhost:2181")
    val accumuloTable = "security"

    val sc = spark.sparkContext
    val writer = new AccumuloLoader(accumuloConf)
    val persisted = sc.parallelize(
      Seq(
        ("Antoine Amend", "Matthew Hallett"),
        ("Matthew Hallett", "Antoine Amend"),
        ("Antoine", "Matthew Hallett"))
    )

    writer.persist(sc, accumuloTable, persisted, Set("Antoine Amend"))

    println("WITH UNRESTRICTED ACCESS")
    val reader1 = new AccumuloReader(accumuloConf)
    val retrieved1 = reader1.read(sc, accumuloTable, Some(AccumuloAuthorization.BLACKLIST))
    retrieved1.cache()
    retrieved1.map(_.toString).foreach(println)
    assert(retrieved1.count() === 3)

    println("WITH RESTRICTED ACCESS")
    val reader2 = new AccumuloReader(accumuloConf)
    val retrieved2 = reader2.read(sc, accumuloTable)
    retrieved2.cache()
    retrieved2.map(_.toString).foreach(println)
    assert(retrieved2.count() === 1)

  }
}
