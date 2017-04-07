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

package io.gzet.tagging.classifier

import java.io.StringReader

import com.typesafe.config.ConfigFactory
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.{HashingTF, Normalizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Create a singleton Classifier where model can be shared across GDELT and Twitter layers
  */
object Classifier {

  final val config = ConfigFactory.load().getConfig("io.gzet.kappa")
  final val vectorSize = config.getInt("vectorSize")

  // Classifier is a Singleton where a global model will be updated from Twitter layer
  // This stays Thread-safe as both Labels and Models are wrapped within a same case class (both are synchronized)
  // GDELT data may access an outdated model, which is perfectly fine, but at least a consistent one
  var model = None: Option[ClassifierModel]

  def train(rdd: RDD[(String, Array[String])]): ClassifierModel = {
    val labeledPoints = buildLabeledPoints(rdd)
    val labels = getLabels(rdd)
    labeledPoints.cache()
    val model = NaiveBayes.train(labeledPoints)
    labeledPoints.unpersist(blocking = false)
    val classifierModel = ClassifierModel(model, labels)
    this.model = Some(classifierModel)
    classifierModel
  }

  private def buildLabeledPoints(rdd: RDD[(String, Array[String])]): RDD[LabeledPoint] = {
    val sc = rdd.sparkContext
    val bLabels = sc.broadcast(getLabels(rdd))
    stem(rdd.keys).zip(rdd.values) mapPartitions { it =>
      val labels = bLabels.value
      val hashingTf = new HashingTF(vectorSize)
      val normalizer = new Normalizer()
      it flatMap { case (body, tags) =>
        val vector = hashingTf.transform(body)
        val normVector = normalizer.transform(vector)
        tags map { tag =>
          val label = labels.getOrElse(tag, 0.0d)
          LabeledPoint(label, normVector)
        }
      }
    }
  }

  private def getLabels(rdd: RDD[(String, Array[String])]): Map[String, Double] = {
    rdd.values
      .flatMap(tags => tags)
      .distinct()
      .zipWithIndex()
      .mapValues(_.toDouble + 1.0d)
      .collectAsMap().toMap
  }

  private def stem(rdd: RDD[String]): RDD[Array[String]] = {

    val replacePunc = """\\W""".r
    val replaceDigitOnly = """\\s\\d+\\s""".r

    rdd mapPartitions { it =>
      val analyzer = new EnglishAnalyzer
      it map { line =>
        val content1 = replacePunc.replaceAllIn(line, " ")
        val content = replaceDigitOnly.replaceAllIn(content1, " ")
        val tReader = new StringReader(content)
        val tStream = analyzer.tokenStream("contents", tReader)
        val term = tStream.addAttribute(classOf[CharTermAttribute])
        tStream.reset()
        val terms = collection.mutable.MutableList[String]()
        while (tStream.incrementToken) {
          if (!term.toString.matches(".*\\d.*") && term.toString.length > 3) {
            terms += term.toString
          }
        }
        tStream.close()
        terms.toArray
      }
    }
  }

  def predict(rdd: RDD[String]): RDD[String] = {
    if (model.isEmpty)
      return rdd.sparkContext.emptyRDD[String]

    val latestModel = model.get
    val vectors = buildVectors(rdd)
    latestModel.predict(vectors)
  }

  private def buildVectors(rdd: RDD[String]): RDD[Vector] = {
    val sc = rdd.sparkContext
    stem(rdd) mapPartitions { it =>
      val hashingTf = new HashingTF(vectorSize)
      val normalizer = new Normalizer()
      it map { body =>
        val vector = hashingTf.transform(body)
        normalizer.transform(vector)
      }
    }
  }

  def predictProbabilities(rdd: RDD[String]): RDD[Map[String, Double]] = {
    if (model.isEmpty)
      return rdd.sparkContext.emptyRDD[Map[String, Double]]

    val latestModel = model.get
    val vectors = buildVectors(rdd)
    latestModel.predictProbabilities(vectors)
  }
}

case class ClassifierModel(model: NaiveBayesModel, labels: Map[String, Double]) {

  def predict(vectors: RDD[Vector]): RDD[String] = {
    val sc = vectors.sparkContext
    val bLabels = sc.broadcast(labels.map(_.swap))
    model.predict(vectors).map({ prediction =>
      bLabels.value.get(prediction).get
    })
  }

  def predictProbabilities(vectors: RDD[Vector]): RDD[Map[String, Double]] = {
    val sc = vectors.sparkContext
    val bLabels = sc.broadcast(labels.map(_.swap))
    model.predictProbabilities(vectors).map({ vector =>
      bLabels.value.toSeq.sortBy(_._1).map(_._2).zip(vector.toArray).toMap
    })
  }

  def save(sc: SparkContext, outputDir: String) = {
    model.save(sc, s"$outputDir/model")
    sc.parallelize(labels.toSeq).saveAsObjectFile(s"$outputDir/labels")
  }

}