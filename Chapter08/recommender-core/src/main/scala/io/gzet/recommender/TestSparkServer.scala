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

package io.gzet.recommender

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

object TestSparkServer extends SparkJob {

  override def runJob(sc: SparkContext, conf: Config): Any = {
    sc.parallelize((1 to 10).map(i => "Success")).collect()
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }

}
