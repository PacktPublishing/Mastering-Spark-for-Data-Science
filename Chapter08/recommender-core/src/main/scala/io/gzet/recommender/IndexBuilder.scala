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

import com.datastax.spark.connector._
import com.typesafe.config.Config
import io.gzet.recommender.Config._
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import spark.jobserver._

object IndexBuilder extends SparkJob {

  override def runJob(sc: SparkContext, conf: Config): Any = {

    val inputDir = conf.getString("input.dir")

    val sampleSizeB = sc.broadcast(SAMPLE_SIZE)
    val audioSongRDD = AudioLibrary.read(inputDir, sc, MIN_TIME, MAX_TIME)
    val songRDD = audioSongRDD.keys.sortBy(song => song).zipWithIndex().mapValues(l => l + 1)
    val songIdsB = sc.broadcast(songRDD.collectAsMap())

    val audioRDD = audioSongRDD mapPartitions { audios =>
      val songIds = songIdsB.value
      audios map { case (song, audio) =>
        (songIds.get(song).get, audio)
      }
    }

    val sampleRDD = audioRDD flatMap { case (songId, audio) =>
      audio.sampleByTime(sampleSizeB.value) map { sample =>
        (songId, sample)
      }
    }

    val recordRDD = songRDD map { case (name, id) =>
      Record(id, name)
    }

    val hashRDD = sampleRDD.map({case (songId, sample) =>
      ((sample.hash, songId), Array(sample.id))
    }).reduceByKey(_ ++ _).mapValues(a => a.mkString(",")).map({case ((hash, songId), sampleIds) =>
      (hash, songId)
    }).groupByKey().mapValues(it => it.toList).map({case (id, songs) =>
      Hash(id, songs)
    })

    hashRDD.saveAsCassandraTable(KEYSPACE, TABLE_HASH)
    recordRDD.saveAsCassandraTable(KEYSPACE, TABLE_RECORD)

  }

  def containsWav(hdfs: FileSystem, path: Path) = {
    val it = hdfs.listFiles(path, false)
    var i = 0
    while(it.hasNext){
      if(it.next().getPath.getName.endsWith(".wav")){
        i += 1
      }
    }
    i > 0
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    if(!config.hasPath("input.dir")) {
      SparkJobInvalid("Missing parameter [input.dir]")
    } else {
      val hdfs = FileSystem.get(sc.hadoopConfiguration)
      val path = new Path(config.getString("input.dir"))
      val isDir = hdfs.isDirectory(path)
      val isValid = containsWav(hdfs, path)
      hdfs.close()
      if(isDir && isValid) {
        SparkJobValid
      } else {
        SparkJobInvalid("Input directory does not contains .wav files")
      }
    }

  }

}
