package com.nitro.iterator.s3

import java.io.FileOutputStream

import com.amazonaws.services.s3.model.S3ObjectInputStream

object CopyStreams {

  type Bytes = Int

  def apply[I, O](input: I, output: O, chunkSize: Bytes = 2048)(implicit r: Reader[I], w: Writer[O]): Unit = {
    val buffer = Array.ofDim[Byte](chunkSize)
    var count = -1

    while ({
      count = r.read(input, buffer); count > 0
    })
      w.write(output, buffer, 0, count)
  }
}

trait Reader[I] {
  def read(input: I, buffer: Array[Byte]): Int
}

object Reader {

  implicit val s3ObjectISReader = new Reader[S3ObjectInputStream] {
    @inline override def read(input: S3ObjectInputStream, buffer: Array[Byte]): Int =
      input.read(buffer)
  }

}

trait Writer[O] {
  def write(output: O, buffer: Array[Byte], startAt: Int, nBytesToWrite: Int): Unit
}

object Writer {

  implicit val fileOSWriter = new Writer[FileOutputStream] {
    @inline override def write(
      output: FileOutputStream,
      buffer: Array[Byte],
      startAt: Int,
      nBytesToWrite: Int): Unit =

      output.write(buffer, startAt, nBytesToWrite)
  }

}
