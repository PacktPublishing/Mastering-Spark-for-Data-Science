package io.gzet.story.util

import java.io.StringReader

import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

object Tokenizer {

  lazy private val replacePunc = """\\W""".r
  lazy private val replaceDigitOnly = """\\s\\d+\\s""".r

  def lucene(url: Traversable[String]): Traversable[Seq[String]] = {
    val analyzer = new EnglishAnalyzer
    url.map({ text =>
      lucene(text, analyzer)
    })
  }

  def lucene(line: String, analyzer: EnglishAnalyzer = new EnglishAnalyzer()): Seq[String] = {
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
    terms.toSeq
  }
}
