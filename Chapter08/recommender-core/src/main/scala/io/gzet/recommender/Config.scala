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

object Config {

  final val KEYSPACE = "music"
  final val TABLE_HASH = "hash"
  final val TABLE_RECORD = "record"
  final val TABLE_NODE = "nodes"
  final val TABLE_EDGE = "edges"
  final val MIN_TIME = 0
  final val MAX_TIME = 60000
  final val SAMPLE_SIZE = 50.0
  final val MIN_SIMILARITY = 0.0
  final val RDD_EDGE = "playlist:edges"
  final val RDD_NODE = "playlist:nodes"
  final val TOLERANCE = 0.001
  final val TELEPORT = 0.1

}
