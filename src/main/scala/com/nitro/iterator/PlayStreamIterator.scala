package com.nitro.iterator

import play.api.libs.iteratee.{ Enumeratee, EnumerateeHelpers, Enumerator, Iteratee }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ ExecutionContext, Future }

/**
 *  A wrapper for the Play Enumerator providing Scala collection-like behavior.
 */
class PlayStreamIterator[T](val underlying: Enumerator[T]) {

  /**
   * Future of option the first element in the stream
   */
  def headOption: Future[Option[T]] = {
    val takeOne: Enumeratee[T, T] = Enumeratee.take[T](1)

    val sink = Iteratee.fold[T, Option[T]](None) { (soFar, incoming) =>
      Some(incoming)
    }

    underlying &> takeOne |>>> sink
  }

  /**
   * Future of option of the first element satisfying a given predicate in the stream
   */
  def find(predicate: (T => Boolean)): Future[Option[T]] = {
    dropWhile(t => !predicate(t)).headOption
  }

  /**
   * Future of option of the first element satisfying a given predicate in the stream
   */
  def findM(predicate: (T => Future[Boolean])): Future[Option[T]] = {
    val notPredicate: (T => Future[Boolean]) = {
      case t => predicate(t) map (res => !res)
    }
    dropWhileM(notPredicate).headOption
  }

  /**
   * Transform elements in the stream to elements of type U
   */
  def map[U](f: T => U): PlayStreamIterator[U] = {
    val transformer = Enumeratee.map[T](f)
    new PlayStreamIterator[U](underlying &> transformer)
  }

  /**
   * Transform elements in the stream to elements of type U
   */
  def mapM[U](f: T => Future[U]): PlayStreamIterator[U] = {
    val transformer = Enumeratee.mapM[T](f)
    new PlayStreamIterator[U](underlying &> transformer)
  }

  /**
   * Apply the side effecting function f to each element in the stream.
   */
  def foreach(f: T => Unit): Future[Unit] = {
    val transformer = Enumeratee.map[T](f)
    val noOpSink = Iteratee.fold[Unit, Unit](()) { (soFar, incoming) =>
      ()
    }
    underlying &> transformer |>>> noOpSink
  }

  /**
   * Filter the stream based on the predicate function
   */
  def filter(predicate: (T => Boolean)): PlayStreamIterator[T] = {
    val filterEnumeratee = Enumeratee.filter[T](predicate)

    new PlayStreamIterator[T](underlying &> filterEnumeratee)
  }

  /**
   * Filter the stream based on the predicate function
   */
  def filterM(predicate: (T => Future[Boolean])): PlayStreamIterator[T] = {
    val transformer = EnumerateeHelpers.filterM(predicate)

    new PlayStreamIterator[T](underlying &> transformer)
  }

  /**
   * Batch the elements of the stream
   */
  def grouped(batchSize: Int): PlayStreamIterator[List[T]] = {
    val batchTransformer: Enumeratee[T, List[T]] = EnumerateeHelpers.batched(batchSize)
    new PlayStreamIterator(underlying &> batchTransformer)
  }

  /**
   * Count all elements in the stream
   */
  def count: Future[Long] = {
    val countingSink = Iteratee.fold[T, Long](0L) {
      case (soFar, elt) => soFar + 1
    }
    underlying |>>> countingSink
  }

  /**
   * Count the elements in the stream satisfying a predicate
   */
  def count(predicate: T => Boolean): Future[Long] = {
    filter(predicate).count
  }

  /**
   * Count the elements in the stream satisfying a predicate
   */
  def countM(predicate: T => Future[Boolean]): Future[Long] = {
    filterM(predicate).count
  }

  /**
   * Apply predicate to all elements of the stream
   * Return a future of boolean that is true
   * only if predicate evaluates to true for all elements.
   * // TODO: make this stop at the first element that is a 'false'
   */
  def forall(predicate: T => Future[Boolean]): Future[Boolean] = {
    val transformer = Enumeratee.mapM[T](predicate)
    val reducerSink = Iteratee.fold[Boolean, Boolean](true) {
      case (soFar, trueOrFalse) =>
        soFar && trueOrFalse
    }

    underlying &> transformer |>>> reducerSink
  }

  /**
   * Returns a stream that contains up to n first elements
   */
  def take(count: Int): PlayStreamIterator[T] = {
    val takeTransformer = Enumeratee.take[T](count)
    new PlayStreamIterator[T](underlying &> takeTransformer)
  }

  /**
   * Returns a stream that continues being evaluated as long the predicate is satisfied
   */
  def takeWhile(predicate: T => Boolean): PlayStreamIterator[T] = {
    val takeWhileTransformer = Enumeratee.takeWhile[T](predicate)
    new PlayStreamIterator[T](underlying &> takeWhileTransformer)
  }

  /**
   * Returns a stream that continues being evaluated as long the predicate is satisfied
   */
  def takeWhileM(predicate: T => Future[Boolean]): PlayStreamIterator[T] = {
    val takeWhileTransformer = EnumerateeHelpers.takeWhileM(predicate)
    new PlayStreamIterator[T](underlying &> takeWhileTransformer)
  }

  /**
   * Drops the first n elements from the stream, returning a stream containing the remaining elements.
   */
  def drop(count: Int): PlayStreamIterator[T] = {
    val dropTransformer = Enumeratee.drop[T](count)
    new PlayStreamIterator[T](underlying &> dropTransformer)
  }

  /**
   * Returns a stream that continues being evaluated as long the predicate is satisfied
   */
  def dropWhile(predicate: T => Boolean): PlayStreamIterator[T] = {
    val dropWhileTransformer = Enumeratee.dropWhile(predicate)
    new PlayStreamIterator[T](underlying &> dropWhileTransformer)
  }

  /**
   * Returns a stream that continues being evaluated as long the predicate is satisfied
   */
  def dropWhileM(predicate: T => Future[Boolean]): PlayStreamIterator[T] = {
    val dropWhileTransformer = EnumerateeHelpers.dropWhileM(predicate)
    new PlayStreamIterator[T](underlying &> dropWhileTransformer)
  }

  /**
   * Fold left that folds over the stream starting with the zero element of type U
   */
  def foldLeft[U](zero: U)(f: (U, T) => U): Future[U] = {
    val foldingSink = Iteratee.fold[T, U](zero)(f)
    underlying |>>> foldingSink
  }

  /**
   * Fold left that folds over the stream starting with the zero element of type U
   */
  def foldLeftM[U](zero: U)(f: (U, T) => Future[U]): Future[U] = {
    val foldingSink = Iteratee.foldM[T, U](zero)(f)
    underlying |>>> foldingSink
  }

  /**
   * Evaluates to a flattened stream where each element T is flattened
   * into a sequence of U types. The resulting stream is of type U.
   */
  def flatMap[U](f: T => PlayStreamIterator[U]): PlayStreamIterator[U] = {
    val flatMap = Enumeratee.mapFlatten[T][U]((element: T) => f(element).underlying)
    new PlayStreamIterator[U](underlying &> flatMap)
  }

  /**
   * Evalautes to a flattened stream. The type T must be something that is
   * TraversableOnce, e.g. an Option, Seq, List, or something else with
   * similar semantics. The resulting stream will have "exploded" or "flattened"
   * each one of these elements.
   */
  def flatten[U](implicit asEnumerator: T => PlayStreamIterator[U]): PlayStreamIterator[U] =
    flatMap(x => asEnumerator(x))

}

object PlayStreamIterator {

  /**
   * Create a stream iterator with the enumerator as underlying stream.
   */
  def apply[T](enumerator: Enumerator[T]) = {
    new PlayStreamIterator[T](underlying = enumerator)
  }

  /**
   * Enumerate the traversable in a stream fashion.
   */
  def traverse[T](traversable: TraversableOnce[T]) = {
    new PlayStreamIterator[T](Enumerator.enumerate(traversable))
  }
}

object PlayStreamIteratorConversions {

  import scala.language.implicitConversions

  /**
   * Allows easy conversion from T => {Option , Seq, List, etc.} to T => Enumerator
   * for use in PlayStreamIterator's flatMap.
   */
  implicit def travOnceFn2EnumeratorFn[T, U](f: T => TraversableOnce[U])(implicit ec: ExecutionContext): T => PlayStreamIterator[U] =
    (input: T) => travOnce2Enumerator(f(input))

  /**
   * Allows for easy conversion of a TraversableOnce (Option, Seq, List, etc.) into an Enumerator.
   * Used in the implicit conversion travOnceFn2Enumeratorfn.
   */
  implicit def travOnce2Enumerator[T](x: TraversableOnce[T])(implicit ec: ExecutionContext): PlayStreamIterator[T] =
    new PlayStreamIterator(Enumerator.enumerate(x))
}

