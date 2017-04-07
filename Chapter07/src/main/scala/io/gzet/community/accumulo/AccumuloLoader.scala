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

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchWriterConfig, ClientConfiguration}
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class AccumuloLoader(config: AccumuloConfig) extends Serializable {

  def persist(sc: SparkContext, accumuloTable: String, rdd: RDD[(String, String)], blacklist: Set[String] = Set()) = {

    val conf = sc.hadoopConfiguration
    val job = Job.getInstance(conf)
    val clientConfig: ClientConfiguration = new ClientConfiguration()
      .withInstance(config.accumuloInstance)
      .withZkHosts(config.zookeeperHosts)

    AccumuloOutputFormat.setConnectorInfo(job, config.accumuloUser, new PasswordToken(config.accumuloPassword))
    AccumuloOutputFormat.setBatchWriterOptions(job, new BatchWriterConfig)
    AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig)
    AccumuloOutputFormat.setCreateTables(job, true)

    val bList = sc.broadcast(blacklist)

    val mutationRdd = rdd.map({ case (from, to) =>
      val visibility = {
        if(bList.value.contains(from) || bList.value.contains(to)){
          new ColumnVisibility(AccumuloAuthorization.BLACKLIST)
        } else {
          new ColumnVisibility("")
        }
      }
      val mutation = new Mutation(from)
      mutation.put("associated", to, visibility, "1")
      (new Text(accumuloTable), mutation)
    })

    mutationRdd.saveAsNewAPIHadoopFile(
      "",
      classOf[Text],
      classOf[Mutation],
      classOf[AccumuloOutputFormat],
      job.getConfiguration
    )

  }


}

object AccumuloAuthorization {
  final val BLACKLIST = "BLACKLIST"
}