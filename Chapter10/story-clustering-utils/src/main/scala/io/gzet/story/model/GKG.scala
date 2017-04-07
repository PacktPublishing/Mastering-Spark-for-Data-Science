package io.gzet.story.model

case class GKG(
                id: String,
                simhash: Int,
                date: Long,
                url: String,
                title: String,
                body: String,
                tone: Double,
                geohashes: Array[String],
                locations: Array[String],
                themes: Array[String],
                persons: Array[String],
                organizations: Array[String]
              )