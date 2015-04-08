package play.api.libs.iteratee

import play.api.libs.iteratee.Execution.Implicits.{ defaultExecutionContext => dec }

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

/**
 * NOTE: Some of these helpers are adapted from the Play source code as Play doesn't allow the predicates to be Futures.
 * Enumeratee helpers
 */
object EnumerateeHelpers {

  /**
   * Batching enumeratee
   *
   */
  def batched[T](size: Int)(implicit ec: ExecutionContext): Enumeratee[T, List[T]] = {
    def helper(chunk: List[T] = Nil): Iteratee[T, List[T]] =
      Cont[T, List[T]] {
        case Input.El(data) if chunk.size == (size - 1) =>
          Done(chunk :+ data)
        case Input.El(data) if chunk.size >= size =>
          Error(s"Unexpected chunk size ${chunk.size}", Input.El(data))
        case Input.El(data) if chunk.size < size =>
          helper(chunk :+ data)
        case Input.EOF =>
          Done(chunk)
        case Input.Empty =>
          helper(chunk)
      }
    Enumeratee.grouped[T](helper(Nil))
  }

  /**
   * Create an Enumeratee that filters the inputs using the given predicate
   *
   * @param predicate A function to filter the input elements.
   * $paramEcSingle
   */
  def filterM[U](predicate: U => Future[Boolean])(implicit ec: ExecutionContext): Enumeratee[U, U] = new Enumeratee.CheckDone[U, U] {
    def step[A](k: K[U, A]): K[U, Iteratee[U, A]] = {

      case in @ Input.El(e) => Iteratee.flatten(predicate(e).map { b =>
        if (b) (new Enumeratee.CheckDone[U, U] { def continue[A](k: K[U, A]) = Cont(step(k)) } &> k(in)) else Cont(step(k))
      }(dec))

      case in @ Input.Empty =>
        new Enumeratee.CheckDone[U, U] { def continue[A](k: K[U, A]) = Cont(step(k)) } &> k(in)

      case Input.EOF => Done(Cont(k), Input.EOF)

    }

    def continue[A](k: K[U, A]) = Cont(step(k))
  }

  /**
   * Create an Enumeratee that passes input through while a predicate is satisfied. Once the predicate
   * fails, no more input is passed through.
   *
   * @param f A predicate to test the input with.
   * $paramEcSingle
   */
  def takeWhileM[E](predicate: E => Future[Boolean])(implicit ec: ExecutionContext): Enumeratee[E, E] = {
    new Enumeratee.CheckDone[E, E] {

      def step[A](k: K[E, A]): K[E, Iteratee[E, A]] = {

        case in @ Input.El(e) => Iteratee.flatten(predicate(e).map {
          b => if (b) (new Enumeratee.CheckDone[E, E] { def continue[A](k: K[E, A]) = Cont(step(k)) } &> k(in)) else Done(Cont(k), in)
        }(dec))

        case in @ Input.Empty =>
          new Enumeratee.CheckDone[E, E] { def continue[A](k: K[E, A]) = Cont(step(k)) } &> k(in)

        case Input.EOF => Done(Cont(k), Input.EOF)
      }

      def continue[A](k: K[E, A]) = Cont(step(k))

    }
  }

  /**
   * Create an Enumeratee that drops input until a predicate is satisfied.
   *
   * @param f A predicate to test the input with.
   * $paramEcSingle
   */
  def dropWhileM[E](p: E => Future[Boolean])(implicit ec: ExecutionContext): Enumeratee[E, E] = {
    new Enumeratee.CheckDone[E, E] {

      def step[A](k: K[E, A]): K[E, Iteratee[E, A]] = {

        case in @ Input.El(e) => Iteratee.flatten(p(e).map {
          b => if (b) Cont(step(k)) else (Enumeratee.passAlong[E] &> k(in))
        }(dec))

        case in @ Input.Empty => Cont(step(k))

        case Input.EOF        => Done(Cont(k), Input.EOF)

      }

      def continue[A](k: K[E, A]) = Cont(step(k))

    }
  }

  def sleep[T](time: FiniteDuration)(ec: ExecutionContext): Enumeratee[T, T] = {
    Enumeratee.map[T] { t =>
      // todo: better than actually putting a thread to sleep
      Thread.sleep(time.toMillis)
      t
    }
  }

  def noop[T](ec: ExecutionContext): Enumeratee[T, T] = Enumeratee.map[T](t => t)
}
