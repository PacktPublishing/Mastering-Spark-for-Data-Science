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

import java.io.{File, FileInputStream}

import org.apache.spark.SparkContext

object AudioLibrary {

  def read(library: String, sc: SparkContext, minTime: Long = 0, maxTime: Long = 20000) = {
    sc binaryFiles library filter { case (file, stream) =>
      file.endsWith(".wav")
    } map { case (file, stream) =>
      val fileName = new File(file).getName
      val audio = Audio.processSong(stream.open(), minTime, maxTime)
      (fileName, audio)
    }
  }

  def readFile(song: String, minTime: Long = 0, maxTime: Long = Long.MaxValue) = {
    val is = new FileInputStream(song)
    Audio.processSong(is, minTime, maxTime)
  }

}
