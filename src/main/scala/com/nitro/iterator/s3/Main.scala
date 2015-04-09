package com.nitro.iterator.s3

import com.nitro.iterator.PlayStreamIterator
import scopt.Read

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._

sealed trait Action

case class PrintKeys(limit: Long) extends Action
case object CountKeys extends Action
case class FilterKeys(limit: Long, keyPredicate: String) extends Action

case class ActionOpt(name: String, limit: Long = -1, extra: String = "") {

  def toAction: Action = {
    val n = name.toLowerCase

    if (n.startsWith("printkeys"))
      PrintKeys(correctedLimit)

    else if (n.startsWith("countkeys"))
      CountKeys

    else if (n.startsWith("filterkeys"))
      FilterKeys(correctedLimit, extra)

    else
      throw new Exception(s"unknown Action name: $name")
  }

  def correctedLimit: Long =
    if (limit < 0)
      Long.MaxValue
    else
      limit

}

case class MainConfig(
    accessKey: String = "",
    secretKey: String = "",
    bucket: String = "",
    action: Option[Action] = None) {
  override def toString: String =
    s"{Access Key: $accessKey , Secret Key: $secretKey , Bucket: $bucket, Action: $action}"
}

object MainConfig {

  implicit val readAction = new Read[ActionOpt] {

    override def arity: Int = 3

    override def reads =
      (s: String) => {
        val bits = s.split(" ")
        if (bits.size == 2)
          ActionOpt(bits(0), bits(1).toLong)
        else
          ActionOpt(bits(0), bits(1).toLong, bits(2))
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

    opt[ActionOpt]('c', "action") required () valueName "<Action>" action { (x, c) =>
      c.copy(action = Some(x.toAction))
    } text "action is what this client will do: CountKeys limit , PrintKeys limit, FilterKeys limit predicate"
  }

  def printKeys(iter: PlayStreamIterator[List[S3ObjectSummary]]): Future[Unit] =
    iter
      .map(l =>
        l.map(x =>
          s"${x.key} ${x.underlying.getSize} ${x.underlying.getStorageClass} ${x.lastModified} "
        )
      )
      .foreach(l => {
        println(s"""${l.mkString("\n")}""")
      })

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

          case PrintKeys(limit) =>
            println(s"Printing up to $limit keys...")
            Await.result(
              printKeys(keys.take(limit.toInt)),
              Duration.Inf
            )

          case CountKeys =>
            println(s"Counting all keys...")
            val nKeys = Await.result(keys.count, Duration.Inf)
            println(s"$nKeys keys in bucket ${bucket.name}")

          case FilterKeys(limit, keyPredicate) =>
            println(s"Filtering keys using predicate on name: $keyPredicate")
            Await.result(
              keys
                .take(limit)
                .map(l =>
                  l.filter(x => x.key.startsWith(keyPredicate))
                ),
              Duration.Inf
            )
        }
      }
  }
}