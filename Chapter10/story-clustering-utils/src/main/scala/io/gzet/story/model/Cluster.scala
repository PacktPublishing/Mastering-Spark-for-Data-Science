package io.gzet.story.model

case class Cluster(
                    localId: Int,
                    date: Long,
                    numArticles: Int,
                    title: String,
                    url: String,
                    avgTone: Double,
                    themes: List[String],
                    countries: List[String],
                    organizations: List[String],
                    persons: List[String]
                  )
