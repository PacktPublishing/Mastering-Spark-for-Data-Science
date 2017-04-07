package io.gzet.story.util

import scala.io.Source

object Stopwords {

  lazy val stopwords = Source.fromInputStream(this.getClass.getResourceAsStream("/stopwords"))
    .getLines()
    .map(s => Tokenizer.lucene(s))
    .map(_.mkString(" "))
    .toSet

}
