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

import io.gzet.profilers.raw.AsciiProfiler
import io.gzet.test.SparkFunSuite
import org.scalatest.Matchers

class AsciiProfilerTest extends SparkFunSuite with Matchers {

  localTest("should recognise basic ascii") { spark =>

    // given
    import spark.implicits._
    val seq = Seq("Hello,\t", "World!")
    val df = seq.toDS()

    // when
    val profiler = AsciiProfiler()
    val report = profiler.profile(df)

    // then
    report.show()
    val rep = report.collect()
    rep.length should equal(seq.flatMap(_.toCharArray).distinct.length)

  }

}
