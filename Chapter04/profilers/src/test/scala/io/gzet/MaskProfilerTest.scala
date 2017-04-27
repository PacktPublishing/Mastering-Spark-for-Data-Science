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

import io.gzet.profilers.field.{MaskBasedProfiler, PredefinedMasks}
import io.gzet.test.SparkFunSuite
import org.scalatest.Matchers

class MaskProfilerTest extends SparkFunSuite with Matchers {

  case class Test(first: String, second: String, third: String, fourth: String, fifth: String) {}

  val sources = Array[String]()
  val ingestTime = new java.util.Date().getTime

  localTest("should count occurrences of ASCIICLASS_LOWGRAIN") { spark =>

    // given
    import spark.implicits._
    val df = asTest(
      "1,ABC,9,01-02-2016,127.0.0.1",
      "1,ABC,9,01-02-2016,192.168.56.1",
      "2,XYZ,8,01-03-2016,192.168.56.0",
      "3,123,7,11-06-2016,255.255.255.255",
      "4,XYZ,6,12-12-2016,127.0.0.1").toDS()

    // when
    val profiler = MaskBasedProfiler(5, PredefinedMasks.ASCIICLASS_LOWGRAIN)
    val report = profiler.profile(df)
    report.show()

    // test
    val rep = report.collect()
    rep.count(_.field == 1) should be(1)
    rep.count(_.field == 2) should be(2)
    rep.count(_.field == 3) should be(1)
    rep.count(_.field == 4) should be(1)
    rep.count(_.field == 5) should be(1)

    rep.filter(_.field == 1).map(_.mask) should contain("9")
    rep.filter(_.field == 2).map(_.mask) should contain("9")
    rep.filter(_.field == 2).map(_.mask) should contain("A")
    rep.filter(_.field == 3).map(_.mask) should contain("9")
    rep.filter(_.field == 4).map(_.mask) should contain("9-9-9")
    rep.filter(_.field == 5).map(_.mask) should contain("9.9.9.9")

  }

  localTest("should count occurrences of ASCIICLASS_HIGHGRAIN") { spark =>

    // given
    import spark.implicits._
    val df = asTest(
      "1,ABC,9,01-02-2016,127.0.0.1",
      "1,AB9,A,01-02-2016,127.0.0.1",
      "1,ABC,9,01-02-2016,192.168.56.1",
      "2,XYZ,8,01-03-2016,192.168.56.0",
      "3,123,7,11-06-2016,255.255.255.0",
      "4,XYZ,6,12-12-2016,127.0.0.1").toDS()

    // when
    val profiler = MaskBasedProfiler(5, PredefinedMasks.ASCIICLASS_HIGHGRAIN)
    val report = profiler.profile(df)
    report.show()

    // test
    val rep = report.collect()
    rep.count(_.field == 1) should be(1)
    rep.count(_.field == 2) should be(3)
    rep.count(_.field == 3) should be(2)
    rep.count(_.field == 4) should be(1)
    rep.count(_.field == 5) should be(3)

    rep.filter(_.field == 1).map(_.mask) should contain("9")
    rep.filter(_.field == 2).map(_.mask) should contain("999")
    rep.filter(_.field == 2).map(_.mask) should contain("AAA")
    rep.filter(_.field == 2).map(_.mask) should contain("AA9")
    rep.filter(_.field == 3).map(_.mask) should contain("9")
    rep.filter(_.field == 3).map(_.mask) should contain("A")
    rep.filter(_.field == 4).map(_.mask) should contain("99-99-9999")
    rep.filter(_.field == 5).map(_.mask) should contain("999.999.999.9")
    rep.filter(_.field == 5).map(_.mask) should contain("999.9.9.9")
    rep.filter(_.field == 5).map(_.mask) should contain("999.999.99.9")

  }


  localTest("should count occurrences of POP CHECK patterns") { spark =>

    // given
    import spark.implicits._
    val df = asTest(
      "1,ABC,9,01-02-2016,127.0.0.1",
      ",,,,",
      "2,XYZ,8,01-03-2016,192.168.56.0",
      "3,123,7,11-06-2016,255.255.255.255",
      ",,,,",
      "4,XYZ,6,12-12-2016,127.0.0.1").toDS()

    // when
    val profiler = MaskBasedProfiler(5, PredefinedMasks.POP_CHECKS)
    val report = profiler.profile(df)
    report.show()
    val rep = report.collect()

    // test
    rep.count(t => t.field == 1 && t.mask == "1") should be(1)
    rep.count(t => t.field == 1 && t.mask == "0") should be(1)
    rep.count(t => t.field == 2 && t.mask == "1") should be(1)
    rep.count(t => t.field == 2 && t.mask == "0") should be(1)
    rep.count(t => t.field == 3 && t.mask == "1") should be(1)
    rep.count(t => t.field == 3 && t.mask == "0") should be(1)
    rep.count(t => t.field == 4 && t.mask == "1") should be(1)
    rep.count(t => t.field == 4 && t.mask == "0") should be(1)
    rep.count(t => t.field == 5 && t.mask == "1") should be(1)
    rep.count(t => t.field == 5 && t.mask == "0") should be(1)

    rep.filter(t => t.field == 1 && t.mask == "1").head.metricValue should be(4)
    rep.filter(t => t.field == 1 && t.mask == "0").head.metricValue should be(2)
    rep.filter(t => t.field == 2 && t.mask == "1").head.metricValue should be(4)
    rep.filter(t => t.field == 2 && t.mask == "0").head.metricValue should be(2)
    rep.filter(t => t.field == 3 && t.mask == "1").head.metricValue should be(4)
    rep.filter(t => t.field == 3 && t.mask == "0").head.metricValue should be(2)
    rep.filter(t => t.field == 4 && t.mask == "1").head.metricValue should be(4)
    rep.filter(t => t.field == 4 && t.mask == "0").head.metricValue should be(2)
    rep.filter(t => t.field == 5 && t.mask == "1").head.metricValue should be(4)
    rep.filter(t => t.field == 5 && t.mask == "0").head.metricValue should be(2)

  }

  localTest("should count occurrences of CLASS_FREQS patterns") { spark =>

    // given
    import spark.implicits._
    val df = asTest(
      "挨,abc,ABC,$@,123",
      "爆,def,DEF,£€,456").toDS()

    // when
    val profiler = MaskBasedProfiler(5, PredefinedMasks.CLASS_FREQS)
    val report = profiler.profile(df)
    report.show()
    val rep = report.collect()

    // test
    rep.count(_.field == 1) should be(1)
    rep.count(_.field == 2) should be(1)
    rep.count(_.field == 3) should be(1)
    rep.count(_.field == 4) should be(1)
    rep.count(_.field == 5) should be(1)

    rep.filter(_.field == 1).head.mask should be("J")
    rep.filter(_.field == 2).head.mask should be("a")
    rep.filter(_.field == 3).head.mask should be("A")
    rep.filter(_.field == 4).head.mask should be("U")
    rep.filter(_.field == 5).head.mask should be("9")

  }

  private def asTest(anon: String*): Seq[Array[String]] = {
    anon.toSeq.map {
      case s => val t = s.split(",", 5)
        Array(t(0), t(1), t(2), t(3), t(4))
    }
  }
}
