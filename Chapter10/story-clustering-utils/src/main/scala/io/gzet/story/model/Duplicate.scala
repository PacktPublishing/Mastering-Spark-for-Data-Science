package io.gzet.story.model

case class Duplicate(
                      hash: Int,
                      body: String,
                      title: String,
                      url: String,
                      related: List[Article]
                    )
