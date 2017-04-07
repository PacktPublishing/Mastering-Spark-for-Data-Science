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

package io.gzet.recommender

import java.io.{BufferedInputStream, ByteArrayOutputStream, InputStream}
import javax.sound.sampled.AudioSystem

import org.apache.commons.math3.complex.Complex
import org.apache.commons.math3.transform.{DftNormalization, FastFourierTransformer, TransformType}

import scala.io.Source

case class Audio(data: Array[Byte], byteFreq: Int, sampleRate: Float, minTime: Long, id: Int = 0) {

  def sampleByTime(duration: Double = 50.0d, padding: Boolean = true): List[Audio] = {
    val size = (duration * byteFreq / 1000.0f).toInt
    sample(size, padding)
  }

  def sample(size: Int = math.pow(2, 20).toInt, padding: Boolean = true): List[Audio] = {
    val samples = Audio.sample(data, size, padding)
    samples.zipWithIndex.map({case (sampleAudio, id) =>
      val firstByte = id * size
      val firstTime = firstByte * 1000L / byteFreq.toLong
      Audio(sampleAudio, byteFreq, sampleRate, firstTime, id)
    })
  }

  def fft(): Array[Complex] = {
    val array = Audio.paddingToPowerOf2(data)
    val transformer = new FastFourierTransformer(DftNormalization.STANDARD)
    transformer.transform(array.map(_.toDouble), TransformType.FORWARD)
  }

  def frequencyDomain(): Array[(Float, Double)] = {
    val transform = fft()
    transform.take(transform.length / 2).zipWithIndex.map({ case (c, id) =>
      val freq = (id + 1) * sampleRate / transform.length
      val amplitude = math.sqrt(math.pow(c.getReal, 2) + math.pow(c.getImaginary, 2))
      val db = 20 * math.log10(amplitude)
      (freq, db)
    }).filter({ case (frequency, power) =>
      frequency >= 20 && frequency <= 20000
    })
  }

  def duration: Double = (data.length + 1) * 1000L / byteFreq.toDouble

  def timeDomain: Array[(Double, Int)] = data.zipWithIndex.map({ case (b, idx) =>
    (minTime + idx * 1000L / byteFreq.toDouble, b.toInt)
  })

  def findPeak: Float = {
    val freqDomain = frequencyDomain()
    freqDomain.sortBy(_._2).reverse.map(_._1).head
  }

  override def toString = {
    s"data: ${data.length}, byteFreq: $byteFreq, sampleRate: $sampleRate, minTime: $minTime, duration: $duration, id: $id"
  }

  def hash: String = {
    val freqDomain = frequencyDomain()
    freqDomain.groupBy({ case (frequency, power) =>
      Audio.getFrequencyBand(frequency)
    }).map({ case (bucket, frequencies) =>
      val dominant = frequencies.map({ case (frequency, power) =>
        (Audio.findClosestNote(frequency.toInt), power)
      }).sortBy({ case (note, power) =>
        power
      }).reverse.head._1
      (bucket, dominant)
    }).toList.sortBy(_._1).map(_._2).mkString("-")
  }

}

object Audio {

  val range = Array(20, 60, 250, 2000, 4000, 6000)
  val notes = Source.fromInputStream(this.getClass.getResourceAsStream("/notes")).getLines().flatMap({ line =>
    val a = line.split("\\t")
    a.tail.map(_.toInt).map({ freq =>
      (freq, a.head)
    })
  }).toSeq.sortBy(_._1)

  def paddingToPowerOf2(data: Array[Byte]): Array[Byte] = {
    val n = math.ceil(math.log(data.length) / math.log(2))
    val optimal = math.pow(2, n).toInt
    val padding = Array.fill[Byte](optimal - data.length)(0)
    data ++ padding
  }

  private def sample(array: Array[Byte], size: Int, padding: Boolean = true): List[Array[Byte]] = {
    val length = array.length
    val (head, remaining) = {
      if(length < size){
        if(padding) {
          (array ++ Array.fill[Byte](size - length)(0), Array[Byte]())
        } else {
          (array, Array[Byte]())
        }
      } else {
        (array.take(size), array.takeRight(length - size))
      }
    }
    if(remaining.isEmpty){
      List(head)
    } else {
      List(head) ++ sample(remaining, size, padding)
    }
  }

  def getFrequencyBand(frequency: Float): Int = {
    Audio.range.filter(f => f <= frequency).zipWithIndex.last._2
  }

  def getNote(frequency: Float): Option[String] = {
    Audio.notes.toMap.get(frequency.toInt)
  }

  def findClosestNote(f: Float): String = {
    val upL = Audio.notes.filter(_._1 >= f)
    val downL = Audio.notes.filter(_._1 <= f)
    if (upL.isEmpty && downL.isEmpty) return "-"
    if (upL.isEmpty) return downL.last._2
    if (downL.isEmpty) return upL.head._2
    val up = upL.head
    val down = downL.last
    if (math.abs(f - up._1) < math.abs(f - down._1)) up._2 else down._2
  }

  def processSong(stream: InputStream, minTime: Long, maxTime: Long): Audio = {

    require(minTime >= 0)
    require(minTime < maxTime)

    val bufferedIn = new BufferedInputStream(stream)
    val out = new ByteArrayOutputStream
    val audioInputStream = AudioSystem.getAudioInputStream(bufferedIn)
    val format = audioInputStream.getFormat
    val sampleRate = format.getSampleRate
    val sizeTmp = Math.rint((format.getFrameRate * format.getFrameSize) / format.getFrameRate).toInt
    val size = (sizeTmp + format.getFrameSize) - (sizeTmp % format.getFrameSize)
    val byteFreq: Int = format.getFrameSize * format.getFrameRate.toInt

    val buffer: Array[Byte] = new Array[Byte](size)

    val maxLength = if (maxTime == Long.MaxValue) Long.MaxValue else byteFreq * maxTime / 1000
    val minLength = byteFreq * minTime / 1000

    var available = true
    var totalRead = 0
    while (available) {
      val c = audioInputStream.read(buffer, 0, size)
      totalRead += c
      if (c > -1 && totalRead >= minLength && totalRead < maxLength) {
        out.write(buffer, 0, c)
      } else {
        if (totalRead >= minLength) {
          available = false
        }
      }
    }

    audioInputStream.close()
    out.close()
    Audio(out.toByteArray, byteFreq, sampleRate, minTime)

  }

}