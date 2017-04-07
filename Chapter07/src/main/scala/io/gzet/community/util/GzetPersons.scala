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

package io.gzet.community.util

import scala.collection.mutable.ArrayBuffer

object GzetPersons {
  def buildTuples(array: Array[String]): Array[(String, String)]  = {
    val holdingArray = ArrayBuffer[String]()
    val n = array.length
    val r = 2
    val data = new Array[String](r)
    combinations(array, holdingArray, data, 0, n - 1, 0, r)

    val result = ArrayBuffer[(String, String)]()
    for (s: String <- holdingArray.toArray) {
      val split: Array[String] = s.split(",")
      result += ((split(0), split(1)))
    }
    result.toArray
  }

  def combinations(input: Array[String], result: ArrayBuffer[String], data: Array[String], start: Int, end: Int, index: Int, r: Int): Unit ={
    if(index == r) {
      var s:String = ""
      for (i <- 0 until r) {
        if (i != 0) {
          s += ","
        }
        s += data(i)
      }
      result += s
     return
    }
    var j = start
    while(j <= end && (end - j + 1) >= (r - index)){
      data(index) = input(j)
      combinations(input, result, data, j + 1, end, index + 1, r)
      j += 1
    }
  }
}