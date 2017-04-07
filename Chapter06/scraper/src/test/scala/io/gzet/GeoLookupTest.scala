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

import io.gzet.GeoLookup.GeoName
import io.gzet.test.SparkFunSuite

class GeoLookupTest extends SparkFunSuite {

  localTest("Greenwich should be in the UK") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile).flatMap(g => g.names.map(name => (name, g)))
    val a1 = Array("Greenwich")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateWithContext(inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.filter(_._2.name == "Greenwich").map(_._2.country.get).take(1).head == "United Kingdom")
  }

  localTest("Major UK cities should be in the UK") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile).flatMap(g => g.names.map(name => (name, g)))
    val a1 = Array("Manchester", "London", "Liverpool", "Birmingham")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateWithContext(inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("United Kingdom"))
  }

  localTest("Greenwich from different countries (including UK) should still be in the UK") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile)
    val a1 = Array("Greenwich", "United Kingdom", "Australia")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateBatchWithContext(sc, inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "Greenwich").count() == 1L)
    assert(retrievedRdd.filter(_._2.name == "Greenwich").map(_._2.country.get).take(1).head == "United Kingdom")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("United Kingdom", "Australia"))
  }

  localTest("Greenwich with different contexts is still in the UK") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile).flatMap(g => g.names.map(name => (name, g)))
    val a1 = Array("Greenwich", "United Kingdom", "Australia")
    val inputRdd = sc.parallelize(a1).zipWithIndex().map(_.swap)
    val retrievedRdd = svc.locateWithContext(inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "Greenwich").count() == 1L)
    assert(retrievedRdd.filter(_._2.name == "Greenwich").map(_._2.country.get).take(1).head == "United Kingdom")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("United Kingdom", "Australia"))
  }

  localTest("Greenwich in US should be in US") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile)
    val a1 = Array("Greenwich", "United States")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateBatchWithContext(sc, inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "Greenwich").count() == 1L)
    assert(retrievedRdd.filter(_._2.name == "Greenwich").map(_._2.country.get).take(1).head == "United States")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("United States"))
  }

  localTest("Greenwich in New York should be in New York") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile).flatMap(g => g.names.map(name => (name, g)))
    val a1 = Array("Greenwich", "New York")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateWithContext(inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "Greenwich").count() == 1L)
    assert(retrievedRdd.filter(_._2.name == "Greenwich").map(_._2.country.get).take(1).head == "United States")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("United States"))
    assert(retrievedRdd.map(_._2.state.getOrElse("")).collect().toSet == Set("New York"))
  }


  localTest("London is the capital of the United Kingdom") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile)
    val a1 = Array("London")
    val inputRdd = sc.parallelize(a1).zipWithIndex().map(_.swap)
    val retrievedRdd = svc.locateBatchWithContext(sc, inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "London").map(_._2.country.get).take(1).head == "United Kingdom")

  }

  localTest("London with dog f***ers is in bloody Canada") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile).flatMap(g => g.names.map(name => (name, g)))
    val a1 = Array("Canada", "London")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateWithContext(inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "London").map(_._2.country.get).take(1).head == "Canada")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("Canada"))
  }

  localTest("Geneva itself should be in Switzerland") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile)
    val a1 = Array("Geneva", "Bern")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateBatchWithContext(sc, inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "Geneve").map(_._2.country.get).take(1).head == "Switzerland")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("Switzerland"))
  }

  localTest("Geneva in the US should be in the US") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile).flatMap(g => g.names.map(name => (name, g)))
    val a1 = Array("Geneva", "United States")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateWithContext(inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "Geneva").map(_._2.country.get).take(1).head == "United States")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("United States"))
  }

  localTest("Manchester itself is in the UK") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile)
    val a1 = Array("Manchester")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateBatchWithContext(sc, inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "Manchester").map(_._2.country.get).take(1).head == "United Kingdom")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("United Kingdom"))
  }

  localTest("Most important Manchester city in the US is in Tennessee") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile).flatMap(g => g.names.map(name => (name, g)))
    val a1 = Array("Manchester", "United States")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateWithContext(inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "Manchester").map(_._2.country.get).take(1).head == "United States")
    assert(retrievedRdd.filter(_._2.name == "Manchester").map(_._2.state.get).take(1).head == "Tennessee")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("United States"))
  }

  localTest("Manchester, Iowa in the US should be in Iowa") { spark =>
    val svc = new GeoLookup()
    val sc = spark.sparkContext
    val geoNameRdd = sc.objectFile[GeoName](this.getClass.getResource("/geonames").getFile)
    val a1 = Array("Manchester", "Iowa", "Kentucky", "United States")
    val inputRdd = sc.parallelize(a1).map(s => (0L, s))
    val retrievedRdd = svc.locateBatchWithContext(sc, inputRdd, geoNameRdd).cache()
    retrievedRdd.collect.foreach(println)
    assert(retrievedRdd.count == a1.length)
    assert(retrievedRdd.filter(_._2.name == "Manchester").map(_._2.country.get).take(1).head == "United States")
    assert(retrievedRdd.filter(_._2.name == "Manchester").map(_._2.state.get).take(1).head == "Iowa")
    assert(retrievedRdd.map(_._2.country.getOrElse("")).collect().toSet == Set("United States"))
  }

}
