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

case class MainConfig(
    accessKey: String = "",
    secretKey: String = "",
    bucket: String = "",
    action: Option[Action] = None) {

  override def toString: String =
    s"{Access Key: $accessKey , Secret Key: $secretKey , Bucket: $bucket, Action: $action}"
}

object Main extends App {

  import Action._

  def printKeys(iter: PlayStreamIterator[S3ObjectSummary]): Future[Unit] =
    iter
      .foreach(x =>
        println(s"${x.key}\t${x.bucketName}\t${x.underlying.getSize}\t${x.underlying.getStorageClass}\t${x.lastModified}")
      )

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

  parser.parse(args, MainConfig()) match {

    case None =>
      System.err.println("Unintelligible configuartion. Exiting.")
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