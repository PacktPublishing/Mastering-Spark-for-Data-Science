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

package io.gzet.community.clustering.louvain

/**
  * Louvain vertex state
  * Contains all information needed for louvain community detection
  */
class VState extends Serializable {

  var community = -1L
  var communitySigmaTot = 0L
  var internalWeight = 0L
  var nodeWeight = 0L
  var changed = false

  override def toString: String = {
    s"{community:$community,communitySigmaTot:$communitySigmaTot,internalWeight:$internalWeight,nodeWeight:$nodeWeight}"
  }

}