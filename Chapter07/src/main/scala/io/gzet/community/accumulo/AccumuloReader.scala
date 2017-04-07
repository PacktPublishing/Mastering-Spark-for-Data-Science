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

package io.gzet.community.accumulo

import org.apache.accumulo.core.client.{IteratorSetting, ClientConfiguration}
import org.apache.accumulo.core.client.mapreduce.{AccumuloInputFormat, AbstractInputFormat, InputFormatBase}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.language.postfixOps

class AccumuloReader(config: AccumuloConfig) extends Serializable {

  def read(sc: SparkContext, accumuloTable: String, authorization: Option[String] = None): RDD[EdgeWritable] = {

    val conf = sc.hadoopConfiguration
    val job = Job.getInstance(conf)
    val clientConfig: ClientConfiguration = new ClientConfiguration()
      .withInstance(config.accumuloInstance)
      .withZkHosts(config.zookeeperHosts)

    AbstractInputFormat.setConnectorInfo(job, config.accumuloUser, new PasswordToken(config.accumuloPassword))
    AbstractInputFormat.setZooKeeperInstance(job, clientConfig)
    if(authorization.isDefined)
      AbstractInputFormat.setScanAuthorizations(job, new Authorizations(authorization.get))

    val is = new IteratorSetting(
      1,
      "summingCombiner",
      "org.apache.accumulo.core.iterators.user.SummingCombiner"
    )

    is.addOption("all", "")
    is.addOption("columns", "associated")
    is.addOption("lossy", "TRUE")
    is.addOption("type", "STRING")

    InputFormatBase.addIterator(job, is)
    InputFormatBase.setInputTableName(job, accumuloTable)

    sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[AccumuloGraphxInputFormat],
      classOf[NullWritable],
      classOf[EdgeWritable]
    ) values

  }
}
