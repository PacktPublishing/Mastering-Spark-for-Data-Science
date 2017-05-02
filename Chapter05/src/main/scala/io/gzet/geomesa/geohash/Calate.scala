package io.gzet.geomesa.geohash

/**
  * Note that intercalate and extracalate are only strict inverses if the input lists to intercalate
  * are either of equal lengths, or the first list is one element longer than the second list.
  */
object Calate {

  /**
    * Interlace two lists.
    *
    * E.g. intercalate(List(1,3,5), List(2,4)) == List(1,2,3,4,5)
    *
    * "Extra" numbers, if the lists have unequal lengths, will be included on the tail of the output list.
    */
  def intercalate[A](a: List[A], b: List[A]): List[A] = a match {
    case h :: t => h :: intercalate(b, t)
    case _      => b
  }

  /**
    * De-interlace two lists.
    *
    * E.g. extracalate(List(1,2,3,4,5)) == (List(1,3,5), List(2,4))
    */
  def extracalate[A](a: Seq[A]): (List[A], List[A]) =
    a.foldRight((List[A](), List[A]())) { case (b, (a1, a2)) => (b :: a2, a1) }

}
