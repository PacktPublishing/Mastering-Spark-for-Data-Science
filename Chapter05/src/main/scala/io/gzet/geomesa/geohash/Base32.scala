package io.gzet.geomesa.geohash

import scala.Array.canBuildFrom

/**
  * A specialised implementation for geohash; e.g. does not check that size of input list of bits is 5.
  *
  * NB At present, only handles lowercase input
  */
object Base32 {

  val BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
  val BITS = Array(16, 8, 4, 2, 1)
  val TODEC = Map(BASE32.zipWithIndex: _*)

  /** Convert list of boolean bits to a base-32 character. Only the first 5 bits are considered.*/
  def toBase32(bin: Seq[Boolean]): Char = BASE32((BITS zip bin).collect { case (x, true) => x }.sum)

  private def intToBits(i: Int) = (4 to 0 by -1) map (x => (i >> x & 1) == 1)

  def isValid(s: String): Boolean = !s.isEmpty() && s.forall(TODEC.contains(_))

  /** Convert a base-32 string to a list of bits (booleans) */
  def toBits(s: String): Seq[Boolean] = (s.flatMap(TODEC andThen intToBits))
}
