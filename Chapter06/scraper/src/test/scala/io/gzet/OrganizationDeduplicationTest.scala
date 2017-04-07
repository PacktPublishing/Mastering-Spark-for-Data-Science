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

import scala.io.Source

class OrganizationDeduplicationTest extends SparkFunSuite {

  localTest("Test Name Strategy") { spark =>
    val svc = new OrganizationDeduplication()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq("islamic state IraqLevant", "islamic state of iraq-and-levant", "islamic state of iraq and levant", "islamic state of iraq and levant"))
    val rdd1 = svc.initialize(rdd)
    val rdd2 = svc.identityDedup(rdd1)
    val rdd3 = svc.stringDedup(rdd2, OrganizationDeduplication.stopWords).cache
    rdd3.collect.foreach(println)
    rdd3.count should be(1)
    rdd3.keys.take(1).head should be("islamic state iraq levant")
  }

  localTest("Test Initials Strategy") { spark =>
    val svc = new OrganizationDeduplication()
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq("isil", "islamic state iraq levant", "islamic state iraq levant", "islamic state iraq levant"))
    val rdd1 = svc.initialize(rdd)
    val rdd2 = svc.identityDedup(rdd1)
    val rdd3 = svc.initialsDedup(rdd2).cache
    rdd3.collect.foreach(println)
    rdd3.count should be(1)
    rdd3.keys.take(1).head should be("islamic state iraq levant")
  }

  localTest("Test All Organizations Strategy") { spark =>
    val expected = Set("Deutsche Bank", "IS", "FBI")
    val sc = spark.sparkContext
    val svc = new OrganizationDeduplication
    val rdd1 = sc.parallelize(Source.fromURL(this.getClass.getResource("/organizations")).getLines().toList).zipWithIndex().map(_.swap)
    val rdd2 = svc.deduplicateWithContext(rdd1).cache
    rdd2.map(_._2).distinct.collect().toSet should be(expected)
    rdd2.collect.foreach(println)
  }

}
