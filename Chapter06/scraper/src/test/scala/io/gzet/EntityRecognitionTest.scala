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

package io.gzet

import io.gzet.test.SparkFunSuite
import org.clulab.processors.corenlp.CoreNLPProcessor

import scala.io.Source

class EntityRecognitionTest extends SparkFunSuite {

  test("Yoko Ono") {
    val text = "Yoko Ono said she and late husband John Lennon shared a close relationship with David Bowie"
    val processor = new CoreNLPProcessor()
    val document = processor.annotate(text)
    document.sentences foreach { sentence =>
      println(sentence.syntacticTree.get)
    }

    /*
        (NNP Yoko)
        (NNP Ono)
        (VBD said)
                (PRP she)
              (CC and)
                (JJ late)
                (NN husband)
                (NNP John)
                (NNP Lennon)
              (VBD shared)
                (DT a)
                (JJ close)
                (NN relationship)
                (IN with)
                  (NNP David)
                  (NNP Bowie)
     */
  }

  localTest("Test NLP Extraction") { spark =>

    val sc = spark.sparkContext
    val rdd = sc.parallelize(Source.fromURL(this.getClass.getResource("/corpus")).getLines().toSeq).zipWithIndex().map(_.swap)
    val entities = new EntityRecognition().extractWithContext(rdd).collect()
    entities.foreach(e => println(e._2.getSentences.mkString(",")))

    val persons = entities.flatMap(_._2.getEntities("PERSON")).distinct
    println(s"P: [${persons.mkString(",")}]")
    persons.head should be("François Hollande")

    val organizations = entities.flatMap(_._2.getEntities("ORGANIZATION")).distinct
    println(s"O: [${organizations.mkString(",")}]")
    organizations.head should be("islamic state")

    val locations = entities.flatMap(_._2.getEntities("LOCATION")).distinct
    println(s"L: [${locations.mkString(",")}]")
    locations.head should be("Paris")

  }
}
