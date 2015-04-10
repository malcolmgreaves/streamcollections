package com.nitro.iterator.s3

import java.io.File

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
    } text "action is what this client will do: CountKeys, PrintKeys limit_items, FilterKeys limit_items predicate, SizeSample limit_MB, ItemSample limit_items, copy toBucket fromFileList"
  }

  def printKeys(iter: PlayStreamIterator[S3ObjectSummary]): Future[Unit] =
    iter
      .foreach(x =>
        println(s"${x.key}\t${x.bucketName}\t${x.underlying.getSize}\t${x.underlying.getStorageClass}\t${x.lastModified}")
      )

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

            case Copy(toBucket, output) =>
              System.err.println(s"Copying keys in ${output.getCanonicalPath} to bucket $toBucket")

              val copyKeys =
                Source.fromFile(output)
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

          }

          Await.result(futureResult, Duration.Inf)
      }
  }
}

object EnumeratorFnHelper {

  import scala.language.implicitConversions

  implicit def traversable2Enumerator[T](x: Traversable[T])(implicit ec: ExecutionContext): Enumerator[T] =
    Enumerator.enumerate(x)

}