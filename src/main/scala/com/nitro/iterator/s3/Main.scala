package com.nitro.iterator.s3

import java.io.File

import com.nitro.iterator.PlayStreamIterator
import scopt.Read

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{ Try, Failure, Success, Random }

sealed trait Action

case class PrintKeys(limit: Int) extends Action

case object CountKeys extends Action

case class FilterKeys(limit: Int, keyStartsWith: String) extends Action

case class RandomSample(limit: Int) extends Action

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

        if (n.startsWith("printkeys")) {
          PrintKeys(correctedLimit(bits(1).toInt))

        } else if (n.startsWith("countkeys")) {
          CountKeys

        } else if (n.startsWith("filterkeys")) {

          FilterKeys(correctedLimit(bits(1).toInt), bits(2))

        } else if (n.startsWith("randomsample")) {
          RandomSample(correctedLimit(bits(1).toInt))

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
    } text "action is what this client will do: CountKeys limit , PrintKeys limit, FilterKeys limit predicate"
  }

  def printKeys(iter: PlayStreamIterator[List[S3ObjectSummary]]): Future[Unit] =
    iter
      .map(l =>
        l.map(x =>
          s"${x.key}\t${x.bucketName}\t${x.underlying.getSize}\t${x.underlying.getStorageClass}\t${x.lastModified}"
        )
      )
      .foreach(l =>
        if (l.nonEmpty)
          println(s"""${l.mkString("\n")}""")
      )

  parser.parse(args, MainConfig()) match {

    case None =>
      System.exit(1)

    case Some(config) =>
      System.err.println(s"Using configuration: $config")

      val s3 = S3Client.withCredentials(S3Credentials(config.accessKey, config.secretKey))

      val bucket = s3.bucket(config.bucket)
      val keys = bucket.allKeys()

      config.action match {

        case None =>
          System.err.println("no or unrecognized action specified...terminating")
          System.exit(1)

        case Some(action) =>

          val futureResult: Future[_] = action match {

            case PrintKeys(limit) =>
              System.err.println(s"Printing up to $limit keys...")
              printKeys(keys.take(limit.toInt))

            case CountKeys =>
              System.err.println(s"Counting all keys...")
              val c = keys.count
              import scala.concurrent.ExecutionContext.Implicits.global
              c.onSuccess({
                case nKeys =>
                  println(s"$nKeys keys in bucket ${bucket.name}")
              })
              c

            case FilterKeys(limit, keyPredicate) =>
              System.err.println(s"Filtering keys that start with: $keyPredicate")
              printKeys(
                keys
                  .take(limit)
                  .map(l =>
                    l.filter(x => x.key.startsWith(keyPredicate))
                  )
              )

            case RandomSample(limit) =>
              System.err.println(s"Taking a random sample of size $limit")
              printKeys(
                keys
                  .filter(_ => Random.nextBoolean())
                  .take(limit)
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

              import scala.concurrent.ExecutionContext.Implicits.global

              copyKeys
                .map(k => (k, bucket.copyObject(k, copyToBucket)))
                .foreach({

                  case (k, f) =>

                    println(s"key: $k")

                    Try(Await.result(f, Duration.Inf)) match {

                      case Success(_) =>
                        println(s"Successfully copied: $k to $toBucket")

                      case Failure(e) =>
                        println(s"Failed to copy $k to $toBucket , reason: $e")
                    }
                })
              Future.successful(None)

          }

          Await.result(futureResult, Duration.Inf)
      }
  }
}