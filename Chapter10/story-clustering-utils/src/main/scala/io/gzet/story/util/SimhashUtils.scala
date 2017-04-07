package io.gzet.story.util

object SimhashUtils {

  def masksWithOneBit(): Set[Int] = {
    (0 to 31).map(offset => 1 << offset).toSet
  }

  def masksWithTwoBits(): Set[Int] = {
    val masks = masksWithOneBit()
    masks.flatMap({e1 =>
      masks.filter( e2 => e1 != e2).map({ e2 =>
        e1 | e2
      })
    })
  }

  val searchmasks = masksWithTwoBits ++ masksWithOneBit ++ Set(0)

  implicit class BitOperations(i1: Int) {

    def distance(i2: Int) = {
      Integer.bitCount(i1 ^ i2)
    }

    def isBitSet(bit: Int): Boolean = {
      ((i1 >> bit) & 1) == 1
    }

    def toHashString: String = {
      String.format("%32s", Integer.toBinaryString(i1)).replace(" ", "0")
    }
  }

  implicit class Simhash(content: String) {

    private def shingles(text: String) = {
      text.split("\\s").sliding(2).map(_.mkString(" ").hashCode()).toArray
    }

    def simhash = {
      val aggHash = shingles(content).flatMap({ hash =>
        Range(0, 32).map({ bit =>
          (bit, if(hash.isBitSet(bit)) 1 else -1)
        })
      }).groupBy(_._1).mapValues(_.map(_._2).sum > 0).toArray
      buildSimhash(0, aggHash)
    }

    private def buildSimhash(simhash: Int, aggBit: Array[(Int, Boolean)]): Int = {
      if(aggBit.isEmpty) return simhash
      val (bit, isSet) = aggBit.head
      val newSimhash = if(isSet) {
        simhash | (1 << bit)
      } else {
        simhash
      }
      buildSimhash(newSimhash, aggBit.tail)
    }

    def weightedSimhash = {
      val features = shingles(content)
      val totalWords = features.length
      val aggHashWeight = features.zipWithIndex.map({case (hash, id) =>
        (hash, 1.0 - id / totalWords.toDouble)
      }).flatMap({ case (hash, weight) =>
        Range(0, 32).map({ bit =>
          (bit, if(hash.isBitSet(bit)) weight else -weight)
        })
      }).groupBy(_._1).mapValues(_.map(_._2).sum > 0).toArray
      buildSimhash(0, aggHashWeight)
    }

  }

}
