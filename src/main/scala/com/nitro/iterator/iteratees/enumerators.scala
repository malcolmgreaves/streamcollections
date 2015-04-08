package com.nitro.iterator.enumerators

import com.nitro.iterator.models.Pageable
import play.api.libs.iteratee.Enumerator

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future, Promise }

/**
 * Enumerators
 */
object Enumerators {

  /**
   * Sequence enumerators into a single enumerator stream in the order they appear in the list.
   */
  def sequenced[T](enumerators: List[Enumerator[T]])(implicit ec: ExecutionContext): Enumerator[T] = {
    enumerators match {
      case head :: tail =>
        tail.foldLeft(head) { (enumSoFar, newEnum) =>
          enumSoFar >>> newEnum
        }
      case Nil =>
        Enumerator.empty[T]
    }
  }

  /**
   * Pagination enumerator
   * Adapted from
   * http://engineering.klout.com/2013/01/iteratees-in-big-data-at-klout/
   */
  def pagingEnumerator[T <: Pageable](cursor: Option[String],
    sleep: Option[FiniteDuration] = None)(paginated: Option[String] => Future[Option[T]])(implicit ec: ExecutionContext): Enumerator[T] = {
    def sleepFor(maybeDuration: Option[FiniteDuration]): Unit = {
      maybeDuration map { duration =>
        //        logger info (s"Sleeping for ${duration.toMillis} ms.")
        Thread.sleep(duration.toMillis)
      }
    }

    var maybeCursor = cursor //Next url to fetch
    Enumerator.fromCallback1[T](
      retriever = { firstCall =>

        val resultFuture: Future[Option[T]] =
          (firstCall, maybeCursor) match {
            case (true, _)                     => paginated(maybeCursor).map { t => sleepFor(sleep); t }(ec)
            case (false, Some(marker: String)) => paginated(maybeCursor).map { t => sleepFor(sleep); t }(ec)
            case (false, None)                 => Promise.successful(None).future
          }

        resultFuture.map { result =>
          maybeCursor = result flatMap (_.next)
          result
        }(ec)
      },
      onComplete = {
        case () => ()
      },
      onError = {
        case ignored => ()
      })(ec)
  }

}

