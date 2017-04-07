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

package io.gzet.community.clustering.wcc

class VState extends Serializable {

  var vId = -1L
  var cId = -1L
  var changed = false
  var txV = 0
  var txC = 0
  var vtxV = 0
  var vtxV_C = 0
  var wcc = 0.0d

  override def toString: String = {
    s"{id:$vId,community:$cId,changed:$changed,txV:$txV,txC:$txC,vtxV:$vtxV,vtxV\\C:$vtxV_C,wcc:$wcc}"
  }

  def cloneState: VState = {
    val v = new VState
    v.vId = vId
    v.cId = cId
    v.changed = changed
    v.txV = txV
    v.txC = txC
    v.vtxV = vtxV
    v.vtxV_C = vtxV_C
    v.wcc = wcc
    v
  }

}