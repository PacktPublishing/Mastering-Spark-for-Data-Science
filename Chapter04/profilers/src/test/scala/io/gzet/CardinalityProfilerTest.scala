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

import io.gzet.profilers.field.CardinalityProfiler
import io.gzet.test.SparkFunSuite
import org.scalatest.Matchers

class CardinalityProfilerTest extends SparkFunSuite with Matchers {

  localTest("Should be able to find emptiness score") { spark =>

    // given
    import spark.implicits._
    val df = asTest(
      "1,1,1,01-02-2016,127.0.0.1",
      "2,1,1,01-02-2016,127.0.0.2",
      "3,1,2,01-03-2016,127.0.0.3",
      "4,1,2,01-03-2016,127.0.0.4").toDS()

    // when
    val profiler = CardinalityProfiler(5)
    val report = profiler.profile(df)
    report.show()

    // test
    val rep = report.collect()
    rep.filter(_.field == 1).head.metricValue should be(1.0)
    rep.filter(_.field == 2).head.metricValue should be(0.25)
    rep.filter(_.field == 3).head.metricValue should be(0.5)
    rep.filter(_.field == 4).head.metricValue should be(0.5)
    rep.filter(_.field == 5).head.metricValue should be(1.0)

  }

  private def asTest(anon: String*): Seq[Array[String]] = {
    anon.toSeq.map {
      case s => val t = s.split(",", 5)
        Array(t(0), t(1), t(2), t(3), t(4))
    }
  }
}
