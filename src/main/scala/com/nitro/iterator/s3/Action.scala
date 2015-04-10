package com.nitro.iterator.s3

import java.io.File

import scopt.Read

import scala.util.Try

sealed trait Action

case class PrintKeys(limit: Int) extends Action

case object CountKeys extends Action

case class FilterKeys(limit: Int, keyStartsWith: String) extends Action

case class ItemSample(limit: Int) extends Action

case class SizeSample(size: Megabyte) extends Action

case class Copy(toBucket: String, from: File) extends Action

case class Download(loc: File, limit: Option[Int]) extends Action

object Action {

  implicit val readAction = new Read[Action] {

    override def arity: Int = 3

    override def reads =
      (s: String) => {
        val bits = s.split(" ")
        val name = bits(0)
        val n = name.toLowerCase

        if (n.startsWith("print")) {
          PrintKeys(correctedLimit(bits(1).toInt))

        } else if (n.startsWith("count")) {
          CountKeys

        } else if (n.startsWith("filter")) {
          FilterKeys(correctedLimit(bits(1).toInt), bits(2))

        } else if (n.startsWith("itemsample")) {
          ItemSample(correctedLimit(bits(1).toInt))

        } else if (n.startsWith("sizesample")) {
          SizeSample(Megabyte(correctedLimit(bits(1).toInt)))

        } else if (n.startsWith("copy")) {
          Copy(bits(1), new File(bits(2)))

        } else if (n.startsWith("download")) {
          Download(new File(bits(1)), Try(bits(2).toInt).toOption)

        } else
          throw new IllegalArgumentException(s"unknown Action name: $name")
      }
  }

  def correctedLimit[N](limit: N)(implicit n: Numeric[N]) =
    if (n.lt(limit, n.zero))
      n.zero
    else
      limit

}

case class Megabyte(x: Int)
