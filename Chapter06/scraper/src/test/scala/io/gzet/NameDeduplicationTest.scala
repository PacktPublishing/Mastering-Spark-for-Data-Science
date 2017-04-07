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

import io.gzet.test.SparkFunSuite
import org.apache.commons.codec.language.DoubleMetaphone

import scala.io.Source

class NameDeduplicationTest extends SparkFunSuite {

  localTest("Test a metaphone") { spark =>
    val sc = spark.sparkContext
    val metaphone = new DoubleMetaphone()
    println(metaphone.doubleMetaphone("David") + "#" + metaphone.doubleMetaphone("Bowie"))
    println(metaphone.doubleMetaphone("David") + "#" + metaphone.doubleMetaphone("Bowi"))
    println(metaphone.doubleMetaphone("Davide") + "#" + metaphone.doubleMetaphone("Bowie"))

  }

  localTest("Test a context Deduplication") { spark =>
    val svc = new NameDeduplication()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq( "Antoine", "Antoine", "Antoine Amend", "Antoine Amend", "Antoine Amend", "Andrew Morgan", "Morgan").map(n => (1L, n)))
    val rdd2 = svc.contextDedup(rdd).cache
    rdd2.collect.foreach(println)
    rdd2.count should be(rdd.count)
    rdd2.values.collect.toSet should be(Set("Antoine Amend", "Andrew Morgan"))
  }

  localTest("Test a context Deduplication with similar names") { spark =>
    val svc = new NameDeduplication()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq( "Barack Obama", "Obama", "Barack", "Michelle Obama", "Michelle").map(n => (1L, n)))
    val rdd2 = svc.contextDedup(rdd).cache
    rdd2.collect.foreach(println)
    rdd2.count should be(rdd.count)
    rdd2.values.collect.toSet should be(Set("Barack Obama", "Obama", "Michelle Obama"))
  }

  localTest("Test Name Strategy1") { spark =>
    val svc = new NameDeduplication()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq("andré françois", "andre francois", "andre francois", "andre francois"))
    val rdd1 = svc.initialize(rdd)
    val rdd2 = svc.identityDedup(rdd1)
    val rdd3 = svc.stringDedup(rdd2, NameDeduplication.stopWords).cache
    rdd3.collect.foreach(println)
    rdd3.count should be(1)
    rdd3.keys.take(1).head should be("andre francois")
  }

  localTest("Test Name Strategy2") { spark =>
    val svc = new NameDeduplication()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq("william-vanderbloemen", "william vanderbloemen", "william vanderbloemen", "william vanderbloemen"))
    val rdd1 = svc.initialize(rdd)
    val rdd2 = svc.identityDedup(rdd1)
    val rdd3 = svc.stringDedup(rdd2, NameDeduplication.stopWords).cache
    rdd3.collect.foreach(println)
    rdd3.count should be(1)
    rdd3.keys.take(1).head should be("william vanderbloemen")
  }

  localTest("Test Name Strategy3") { spark =>
    val svc = new NameDeduplication()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq("mr william vanderbloemen", "william vanderbloemen", "william vanderbloemen", "william vanderbloemen"))
    val rdd1 = svc.initialize(rdd)
    val rdd2 = svc.identityDedup(rdd1)
    val rdd3 = svc.stringDedup(rdd2, NameDeduplication.stopWords).cache
    rdd3.collect.foreach(println)
    rdd3.count should be(1)
    rdd3.keys.take(1).head should be("william vanderbloemen")
  }

  localTest("Test Nickname Strategy") { spark =>
    val svc = new NameDeduplication()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq("will vanderbloemen", "william vanderbloemen", "william vanderbloemen", "william vanderbloemen"))
    val rdd1 = svc.initialize(rdd)
    val rdd2 = svc.identityDedup(rdd1)
    val rdd3 = svc.metaphoneDedup(rdd2).cache
    rdd3.collect.foreach(println)
    rdd3.count should be(1)
    rdd3.keys.take(1).head should be("william vanderbloemen")
  }

  localTest("Test Name deduplication") { spark =>
    val expected = Set("Timothy McGinty", "Aaron Franklin", "Stephen Tully", "William Vanderbloemen", "Andre Francois")
    val svc = new NameDeduplication
    val sc = spark.sparkContext
    val inputRdd = sc.parallelize(Source.fromURL(this.getClass.getResource("/persons")).getLines().toSeq).zipWithIndex().map(_.swap)
    val outputRdd = svc.deduplicateWithContext(inputRdd).cache()
    outputRdd.foreach(println)
    outputRdd.map(_._2).distinct.collect().toSet should be(expected)
  }

}
