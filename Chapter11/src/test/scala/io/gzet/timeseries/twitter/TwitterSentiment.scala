package io.gzet.timeseries.twitter

import io.gzet.test.SparkFunSuite
import org.json4s.DefaultFormats
import org.scalatest.Matchers
import io.gzet.timeseries.twitter.Twitter._

import scala.io.Source

class TwitterSentiment extends SparkFunSuite with Matchers {
  test("test stemmer") {
    val text = "I can't believe this. I'd rather not"
    println(stem(text))
    stem(text) should be("i can not believe this . i would rather not")
  }
}
