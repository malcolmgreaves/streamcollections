package com.nitro.iterator.s3

import scala.concurrent.Await
import scala.concurrent.duration._

sealed trait Action

case object PrintKeys extends Action
case object CountKeys extends Action
case class Filter(keyPredicate: String) extends Action

case class MainConfig(
    accessKey: String = "",
    secretKey: String = "",
    bucket: String = "",
    action: Option[Action] = None) {
  override def toString: String =
    s"{Access Key: $accessKey , Secret Key: $secretKey , Bucket: $bucket, Action: $action}"
}

object Main extends App {

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

    opt[String]('c', "action") required () valueName "<Action>" action { (x, c) =>
      if (x.startsWith("PrintKeys")) {
        c.copy(action = Some(PrintKeys))

      } else if (x.startsWith("CountKeys")) {
        c.copy(action = Some(CountKeys))

      } else if (x.startsWith("Filter(")) {
        c.copy(action = Some(PrintKeys))

      } else {
        println(s"Unrecognized Action: $x")
        c
      }
    }
  }

  parser.parse(args, MainConfig()) match {

    case None =>
      System.exit(1)

    case Some(config) =>
      println(s"Using configuration: $config")
      val s3 = S3Client.withCredentials(S3Credentials(config.accessKey, config.secretKey))
      val bucket = s3.bucket(config.bucket)
      val keys = bucket.allKeys()

      config.action match {

        case None =>
          println("no or unrecognized action specified...terminating")
          System.exit(1)

        case Some(action) => action match {

          case PrintKeys =>
            println("Printing all keys...")
            Await.result(
              keys.foreach(l =>
                println(s"""${l.mkString(",")}""")
              ),
              Duration.Inf
            )

          case CountKeys =>
            println("Counting all keys...")
            val nKeys = Await.result(keys.count, Duration.Inf)
            println(s"$nKeys keys in bucket ${bucket.name}")

          case Filter(keyPredicate) =>
            println(s"Filtering keys using predicate on name: $keyPredicate")

        }
      }
  }
}