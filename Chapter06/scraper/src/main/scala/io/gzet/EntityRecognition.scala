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

import org.apache.spark.rdd.RDD
import org.clulab.processors.{Sentence, Processor}
import org.clulab.processors.fastnlp.FastNLPProcessor

class EntityRecognition extends Serializable {

  def extract(corpusRdd: RDD[String]): RDD[Entities] = {
    corpusRdd mapPartitions { case it =>
      val processor = new FastNLPProcessor()
      it map { corpus =>
        val entities = extractEntities(processor, corpus)
        Entities(entities)
      }
    }
  }

  def extractWithContext(corpusRdd: RDD[(Long, String)]): RDD[(Long, Entities)] = {
    corpusRdd mapPartitions { case it =>
      val processor = new FastNLPProcessor()
      it map { case (id, corpus) =>
        val entities = extractEntities(processor, corpus)
        (id, Entities(entities))
      }
    }
  }

  private def extractEntities(processor: Processor, corpus: String) = {
    val doc = processor.annotate(corpus)
    doc.sentences map processSentence
  }

  private def aggregate(entities: Array[Entity]) = {
    aggregateEntities(entities.head, entities.tail)
      .filter(_.eType != "O")
  }

  private def aggregateEntities(current: Entity, entities: Array[Entity], processed : List[Entity] = List[Entity]()): List[Entity] = {
    if(entities.isEmpty) {
      current :: processed
    } else {
      val entity = entities.head
      if(entity.eType == current.eType) {
        val agg = Entity(current.eType, current.eVal + " " + entity.eVal)
        aggregateEntities(agg, entities.tail, processed)
      } else {
        aggregateEntities(entity, entities.tail, current :: processed)
      }
    }
  }

  private def processSentence(sentence: Sentence): List[Entity] = {
    val entities = sentence.lemmas.get
      .zip(sentence.entities.get)
      .map { case (eVal, eType) =>
        Entity(eType, eVal)
      }

    aggregate(entities)
  }

}

case class Entity(eType: String, eVal: String)
case class Entities(sentences: Array[List[Entity]]) {

  def getSentences = sentences

  def getEntities(eType: String) = {
    sentences flatMap { sentence =>
      sentence
    } filter { entity =>
      eType == entity.eType
    } map { entity =>
      entity.eVal
    }
  }

}