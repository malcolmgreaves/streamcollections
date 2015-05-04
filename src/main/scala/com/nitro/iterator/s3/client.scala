package com.nitro.iterator.s3

import java.util.{ Iterator => JIterator, Date }

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.{ S3ObjectSummary => JObjectSummary, _ }
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3Client }
import com.nitro.iterator.PlayStreamIterator
import com.nitro.iterator.enumerators.Enumerators
import com.nitro.iterator.logging._
import com.nitro.iterator.s3.throttling.BackoffThrottling
import com.nitro.iterator.timing.Timing._
import play.api.libs.iteratee.Enumerator
import resource._

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

/**
 * Asynchronous s3 client allowing streaming traversal of the s3 bucket.
 */
class Bucket(val name: String, val client: AmazonS3) {

  /**
   * Default batch size when iterating S3 keys
   */
  lazy val DefaultBatchSize: Int = 1000
  /**
   * Default pause between reads to S3
   */
  lazy val ReadSleep: FiniteDuration = 150.milliseconds

  /**
   * Enumerate all keys in the bucket (uses the built in S3 iterator)
   */
  def allKeys(batchSize: Int = DefaultBatchSize): PlayStreamIterator[List[S3ObjectSummary]] = {

    val objectsIterator: JIterator[JObjectSummary] = S3Objects.inBucket(client, name).withBatchSize(batchSize).iterator
    // implicit conversion to the Scala iterator happens here.
    val scalaIterator: Iterator[S3ObjectSummary] = objectsIterator.map(underlying => S3ObjectSummary(underlying))

    val streams = PlayStreamIterator.traverse[S3ObjectSummary](scalaIterator)

    streams.grouped(batchSize)
  }

  /**
   * Get keys with specified prefix in a paginated fashion.
   * @param prefix prefix for the keys
   * @param cursor offset marker
   * @param delimiter delimiter
   * @param batchSize batch size
   */
  private def getKeysWithPrefix(prefix: String,
    cursor: Option[String] = None,
    delimiter: Option[String] = None,
    batchSize: Option[Int] = None): Future[S3ObjectListing] = {
    val listObjectsRequest = new ListObjectsRequest
    listObjectsRequest.withBucketName(name).withPrefix(prefix)

    batchSize foreach (l => listObjectsRequest.withMaxKeys(l))
    cursor foreach (m => listObjectsRequest.withMarker(m))
    delimiter foreach (d => listObjectsRequest.withDelimiter(d))

    logger.info(s"Getting keys with prefix [$prefix], cursor [$cursor], batchSize [$batchSize]")
    val initialSleep = ReadSleep.toMillis.toInt
    val maxRetries = 10
    val backOffCoefficient = 2
    val maxSleep = initialSleep * StrictMath.pow(2, maxRetries).toInt
    val listingsF =
      BackoffThrottling.throttledAsync(maxRetries = maxRetries, initialSleep = initialSleep, maxSleep = maxSleep, backOffCoefficient = backOffCoefficient.toDouble) {
        Future {
          time(name = s"S3Objects.listObjects.prefix.$prefix.batch.$batchSize") {
            S3ObjectListing(client.listObjects(listObjectsRequest))
          }
        }
      } recoverWith {
        case NonFatal(t: Throwable) =>
          logger.error(s"Throttling failed at cursor $cursor, prefix $prefix & batch size $batchSize", t)
          Future.failed(t)

      }
    listingsF
  }

  def keysWithPrefix(prefix: String,
    marker: Option[String] = None,
    delimiter: Option[String] = None,
    batchSize: Option[Int] = None,
    sleep: Option[FiniteDuration] = None): PlayStreamIterator[List[S3ObjectSummary]] = {

    val paging: Enumerator[S3ObjectListing] = Enumerators.pagingEnumerator[S3ObjectListing](cursor = marker, sleep = sleep) {
      case Some(nextCursor: String) =>
        val listingFuture = getKeysWithPrefix(prefix = prefix, batchSize = Some(DefaultBatchSize), cursor = Some(nextCursor))

        for {
          listing <- listingFuture
        } yield {
          listing.objectSummaries match {
            case Nil          => None
            case head :: tail => Some(listing)
          }
        }
      case None =>
        getKeysWithPrefix(prefix = prefix, batchSize = Some(DefaultBatchSize)) map (res => Some(res))
    }

    val iterator: PlayStreamIterator[S3ObjectListing] = new PlayStreamIterator[S3ObjectListing](paging)

    iterator.map(_.objectSummaries)
  }

  /**
   * Delete object by key in the bucket
   */
  def deleteObject(key: String): Future[Unit] = Future {
    time(s"S3Bucket.deleteObject[$key]") {
      client.deleteObject(name, key)
    }
  }

  val - = deleteObject _

  /**
   * Delete objects under specified keys
   */
  def deleteObjects(keys: List[String]): Future[Unit] = Future {
    time(s"S3bucket.deleteObjects.size.${keys.size}") {
      val request = new DeleteObjectsRequest(name)
      request.withKeys(keys: _*).setQuiet(true)
      client.deleteObjects(request)
    }
  }

  val -- = deleteObjects _

  /**
   * Copy an object from one bucket to this bucket.
   *
   * If the buckets are on the same AWS account, the copy will occur
   * as an asynchonous computation on the AWS backend. Otherwise, the
   * data will be copied to this JVM and then sent back to AWS to be
   * put into the bucket.
   */
  def copyObject(key: String, other: Bucket): Future[Unit] = Future {
    time(s"S3Bucket.copyObject[$key]") {

      Try {
        if (client.getS3AccountOwner.getId == other.client.getS3AccountOwner.getId) {
          // same AWS account, can do backend async copy
          client.copyObject(name, key, other.name, key)

        } else {
          // different AWS accounts, must stream the data into this JVM and then send
          // it off to be copied
          for (is <- managed(client.getObject(name, key).getObjectContent)) {
            other.client.putObject(other.name, key, is, new ObjectMetadata())
          }
        }
      } match {

        // Nothing to do - it's a file copy, so a pure side effect
        case Success(_) =>
          logger info s"Copied $key to bucket ${other.name}"

        // Handle non-fatal errors
        case Failure(NonFatal(t)) =>
          logger error s"Failed to copy $key from bucket $name to bucket ${other.name}:: $t}"
      }
      () // Future[Unit]
    }
  }

  val --> = copyObject _

  def copyObjects(objects: List[String], other: Bucket) = {
    timeAsync(s"S3Bucket.copyObjects[$objects]") {

      Future.sequence(objects.map(obj => copyObject(obj, other)))
    }
  }

  val ---> = copyObjects _

}

object Bucket {

  def apply(name: String, client: AmazonS3) = new Bucket(name, client)
}

class S3Client(val underlying: AmazonS3) {

  def bucket(name: String): Bucket = Bucket(name, underlying)

}

object S3Client {

  def withCredentials(credentials: S3Credentials, configuration: Option[ClientConfiguration] = None): S3Client = {
    val aWSCredentials = new BasicAWSCredentials(credentials.accessKey, credentials.secretKey)
    val s3Client: AmazonS3 = configuration match {
      case Some(config: ClientConfiguration) =>
        new AmazonS3Client(aWSCredentials, config)
      case None =>
        new AmazonS3Client(aWSCredentials)
    }

    new S3Client(s3Client)

  }
}

case class S3Credentials(accessKey: String, secretKey: String)