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

import io.gzet.profilers.raw.StructuralProfiler
import io.gzet.test.SparkFunSuite
import org.scalatest.Matchers

class StructuralProfilerTest extends SparkFunSuite with Matchers {

  localTest("should parse tabular structure") { spark =>

    // given
    import spark.implicits._
    val df = Seq("1,ABC,T,-1", "2,XYZ,F,-1", "3,123,T,-1").toDS()

    // when
    val profiler = StructuralProfiler()
    val report = profiler.profile(df)
    report.show()
    val rep = report.collect()

    // then
    rep.length should equal(1)
    val record = rep.toList.sortBy(_.metricValue).reverse.head
    record.fields should be(4)
    record.metricValue should be(3)
  }

  localTest("should detect anomalies in tabular structure") { spark =>

    // given
    import spark.implicits._
    val df = Seq("1,ABC,T,-1", "2,XYZ,F", "3,123,T,-1").toDS()

    // when
    val profiler = StructuralProfiler()
    val report = profiler.profile(df)
    report.show()

    // then
    report.count should equal(2)
    val head = report.collect().toList.sortBy(_.metricValue).reverse.head
    val last = report.collect().toList.sortBy(_.metricValue).reverse.last
    head.fields should be(4)
    head.metricValue should be(2)
    last.fields should be(3)
    last.metricValue should be(1)
  }

}
