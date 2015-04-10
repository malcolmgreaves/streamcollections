package com.nitro.iterator.s3

import java.io._
import java.nio.file.{ Path, Files }

import com.amazonaws.services.s3.model.{ S3ObjectInputStream, S3Object }
import com.nitro.iterator.PlayStreamIterator
import play.api.libs.iteratee.{ Iteratee, Enumerator }
import scopt.Read

import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{ Try, Failure, Success, Random }

sealed trait Action

case class PrintKeys(limit: Int) extends Action

case object CountKeys extends Action

case class FilterKeys(limit: Int, keyStartsWith: String) extends Action

case class ItemSample(limit: Int) extends Action

case class SizeSample(size: Megabyte) extends Action

case class Megabyte(x: Int)

case class Copy(toBucket: String, from: File) extends Action

case class Download(loc: File, limit: Option[Int]) extends Action

case class MainConfig(
    accessKey: String = "",
    secretKey: String = "",
    bucket: String = "",
    action: Option[Action] = None) {
  override def toString: String =
    s"{Access Key: $accessKey , Secret Key: $secretKey , Bucket: $bucket, Action: $action}"
}

object MainConfig {

  import Numeric._

  def correctedLimit[N](limit: N)(implicit n: Numeric[N]) =
    if (n.lt(limit, n.zero))
      n.zero
    else
      limit

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
          throw new Exception(s"unknown Action name: $name")
      }
  }

}

object Main extends App {

  import MainConfig._

  val parser = new scopt.OptionParser[MainConfig]("Main S3 Client") {
    head("Main S3 Client", "0.1x")

    opt[String]('a', "accessKey") required () valueName "<String>" action { (x, c) =>
      c.copy(accessKey = x)
    } text "accessKey is the S3 access key"

    opt[String]('s', "secretKey") required () valueName "<String>" action { (x, c) =>
      c.copy(secretKey = x)
    } text "secretKey is the S3 secret key"

    opt[String]('b', "bucket") required () valueName "<String>" action { (x, c) =>
      c.copy(bucket = x)
    } text "bucket is the S3 bucket"

    opt[Action]('c', "action") required () valueName "<Action>" action { (x, c) =>
      c.copy(action = Some(x))
    } text "action is what this client will do: CountKeys, PrintKeys limit_items, FilterKeys limit_items predicate, SizeSample limit_MB, ItemSample limit_items, copy toBucket fromFileList, download to_directory limit_or_none"
  }

  def printKeys(iter: PlayStreamIterator[S3ObjectSummary]): Future[Unit] =
    iter
      .foreach(x =>
        println(s"${x.key}\t${x.bucketName}\t${x.underlying.getSize}\t${x.underlying.getStorageClass}\t${x.lastModified}")
      )

  trait Reader[O] {
    def read(input: O, buffer: Array[Byte]): Int
  }

  object Reader {

    implicit val s3ObjectISReader = new Reader[S3ObjectInputStream] {
      override def read(input: S3ObjectInputStream, buffer: Array[Byte]): Int =
        input.read(buffer)
    }

  }

  trait Writer[O] {
    def write(output: O, buffer: Array[Byte], startAt: Int, nBytesToWrite: Int): Unit
  }

  object Writer {

    implicit val fileOSWriter = new Writer[FileOutputStream] {
      override def write(output: FileOutputStream, buffer: Array[Byte], startAt: Int, nBytesToWrite: Int): Unit =
        output.write(buffer, startAt, nBytesToWrite)
    }

  }

  object CopyStreams {
    def apply[I, O](input: I, output: O, chunk: Int = 2048)(implicit r: Reader[I], w: Writer[O]): Unit = {
      val buffer = Array.ofDim[Byte](chunk)
      var count = -1

      while ({
        count = r.read(input, buffer); count > 0
      })
        w.write(output, buffer, 0, count)
    }
  }

  parser.parse(args, MainConfig()) match {

    case None =>
      System.exit(1)

    case Some(config) =>
      System.err.println(s"Using configuration:\n$config")

      val s3 = S3Client.withCredentials(S3Credentials(config.accessKey, config.secretKey))

      val bucket = s3.bucket(config.bucket)
      val keys: PlayStreamIterator[List[S3ObjectSummary]] = bucket.allKeys()

      config.action match {

        case None =>
          System.err.println("no or unrecognized action specified...terminating")
          System.exit(1)

        case Some(action) =>

          import scala.concurrent.ExecutionContext.Implicits.global
          import EnumeratorFnHelper.traversable2Enumerator

          val futureResult: Future[_] = action match {

            case PrintKeys(limit) =>
              System.err.println(s"Printing up to $limit keys...")
              printKeys(
                keys
                  .flatten
                  .take(limit.toInt)
              )

            case CountKeys =>
              System.err.println(s"Counting all keys...")
              keys
                .foldLeft((0L, 0))({
                  case ((nKeys, nCompound), objects) =>
                    (nKeys + objects.size.toLong, nCompound + 1)
                })
                .map({
                  case (nKeys, nCompound) =>
                    println(s"""There are $nKeys keys across $nCompound object lists in bucket "${bucket.name}"""")
                })

            case FilterKeys(limit, keyPredicate) =>
              System.err.println(s"Filtering keys that start with: $keyPredicate")
              printKeys(
                keys
                  .flatten
                  .filter(x => x.key.startsWith(keyPredicate))
                  .take(limit)
              )

            case ItemSample(limit) =>
              System.err.println(s"Taking a random sample of size $limit")
              printKeys(
                keys
                  .flatten
                  .filter(_ => Random.nextBoolean())
                  .take(limit)
              )

            case SizeSample(Megabyte(sizeMB)) =>
              System.err.println(s"Taking a random sample of at least $sizeMB MB")

              val mutableSizePredicate = {

                val sizeInKb = sizeMB * 1000l
                var accumSizeInKB = 0l

                val convObjSizeBytesToKB =
                  (o: S3ObjectSummary) => o.underlying.getSize / 1000 // trunacate is ok

                (o: S3ObjectSummary) =>
                  if (accumSizeInKB > sizeInKb) {
                    System.err.println(s"size exceeded, final accumulated size: $accumSizeInKB KB")
                    false

                  } else {
                    accumSizeInKB += convObjSizeBytesToKB(o)
                    true
                  }
              }

              printKeys(
                keys
                  .flatten
                  .filter(_ => Random.nextBoolean())
                  .takeWhile(mutableSizePredicate)
              )

            case Copy(toBucket, copyFi) =>
              if (!copyFi.isFile) {
                throw new IllegalArgumentException(s"Copy file $copyFi doesn't exist or it is a directory")
              }
              System.err.println(s"Copying keys in ${copyFi.getCanonicalPath} to bucket $toBucket")

              val copyKeys =
                Source.fromFile(copyFi)
                  .getLines()
                  .map(_.split("\\t")(0).trim)
                  .toSeq
              System.err.println(s"${copyKeys.size} keys to copy")

              val copyToBucket = s3.bucket(toBucket)

              copyKeys
                .map(k => (k, bucket.copyObject(k, copyToBucket)))
                .foreach({
                  case (k, f) =>
                    println(s"key: $k")
                    Await.result(f, Duration.Inf)
                })
              Future { None }

            case Download(output, l) =>
              if (output.exists()) {
                if (output.isFile) {
                  throw new IllegalStateException(s"Output exists, but it's a file: $output")
                }
                // it's a directory, so that's ok
              } else {
                // make the directory
                if (!output.mkdirs()) {
                  throw new IllegalStateException(s"Could not create output directory: $output")
                }
              }

              val dlKeys = l match {

                case Some(limit) =>
                  System.err.println(s"Downloading $limit keys to $output")
                  keys.flatten.take(limit)

                case None =>
                  System.err.println(s"Downloading all keys to $output")
                  keys.flatten
              }

              // download each S3 object
              import Reader.s3ObjectISReader
              import Writer.fileOSWriter
              dlKeys.foreach(key =>
                for (
                  is <- resource.managed(bucket.client.getObject(bucket.name, key.key).getObjectContent);
                  fos <- resource.managed(new FileOutputStream(new File(output, key.key)))
                ) {
                  Try(CopyStreams(is, fos)) match {
                    case Success(_) =>
                      ()
                    case Failure(e) =>
                      System.err.println(s"Failed to download $key : $e")
                  }
                }
              )

          }

          Try(Await.result(futureResult, Duration.Inf)) match {

            case Success(_) =>
              ()

            case Failure(t) =>
              System.err.println(s"Error performing $action : $t")
          }
      }
  }
}

object EnumeratorFnHelper {

  import scala.language.implicitConversions

  implicit def traversable2Enumerator[T](x: Traversable[T])(implicit ec: ExecutionContext): Enumerator[T] =
    Enumerator.enumerate(x)

}