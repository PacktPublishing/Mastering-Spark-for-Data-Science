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

package io.gzet.oldapi

import io.gzet.test.SparkFunSuite
import org.io.gzet.gdelt.gkg.v2.SourceCollectionIdentifier

class TestOldApiFunctionality extends SparkFunSuite {

  val gkgRecordIdTranslatedString = "20160101000000-T15"
  val gkgRecordIdUnTranslatedString = "20160101204535-5"

  localTest("Test createGkgRecordIdWithTranslatedString") { sc =>
    val gkgRecordId = createAvro.createGkgRecordId(gkgRecordIdTranslatedString)
    assertResult(gkgRecordId.getDate.toString) ("20160101000000")
    assertResult(gkgRecordId.getTranslingual) (true)
    assertResult(gkgRecordId.getNumberInBatch) (15)
  }

  localTest("Test createGkgRecordIdWithUnTranslatedString") { sc =>
    val gkgRecordId = createAvro.createGkgRecordId(gkgRecordIdUnTranslatedString)
    assertResult(gkgRecordId.getDate.toString) ("20160101204535")
    assertResult(gkgRecordId.getTranslingual) (false)
    assertResult(gkgRecordId.getNumberInBatch) (5)
  }

  localTest("Test createSourceCollectionIdentifier") { sc =>
    val num = "2"
    assertResult(createAvro.createSourceCollectionIdentifier(num))(SourceCollectionIdentifier.CITATIONONLY)
  }

  localTest("Test createV1Count") { sc =>
    val str = "ARREST#99#Something happened today in Belgium#1#Belgium#BG#AB#12.5833#-2745636#ZZ"
    val v1Count = createAvro.createV1Count(str)
    assertResult(v1Count.getCount) (99)
    assertResult(v1Count.getV1Location.getLocationType) (1)

  }
}
