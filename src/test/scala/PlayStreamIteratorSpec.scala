package test

import com.nitro.iterator.{ PlayStreamIteratorConversions, PlayStreamIterator }
import org.specs2.mutable._
import play.api.libs.iteratee.Enumerator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Await, Future }

import org.specs2.specification.Scope

trait EnumeratorFixture extends Scope {

  import scala.language.postfixOps

  val integerEnumerator = Enumerator.enumerate[Int](1 to 5 toList)

  val emptyEnumerator = Enumerator.enumerate[Int](Nil)
}

trait EnumeratorListFixture extends Scope {

  val (intListEnumerator, flattenedIntListEnumerator, sizeIntsInLists) = {
    val x = (0 until 4).map(i => (1 to 5).map(x => x * i)).toSeq
    val flat = x.flatten
    (
      Enumerator.enumerate[Seq[Int]](x),
      Enumerator.enumerate[Int](flat),
      flat.size
    )
  }

}

class PlayStreamIteratorSpec extends Specification {

  import PlayStreamIteratorConversions.{ travOnceFn2EnumeratorFn, travOnce2Enumerator }

  "PlayStreamIterator" should {

    "Return first option of first element of a non-empty stream" in new EnumeratorFixture {
      val iterator = new PlayStreamIterator(integerEnumerator)

      val headOptionF = iterator.headOption
      headOptionF must beSome(1).await
    }

    "Return None when stream is empty" in new EnumeratorFixture {
      val future = new PlayStreamIterator(emptyEnumerator).headOption

      future must beNone.await
    }

    "Find element in a stream using predicate" in new EnumeratorFixture {
      val findF = new PlayStreamIterator[Int](integerEnumerator).find(_ % 3 == 0)
      findF must beSome(3).await
    }

    "Find element in a stream using future predicate" in new EnumeratorFixture {
      val findF = new PlayStreamIterator[Int](integerEnumerator).findM(el => Future(el % 3 == 0))
      findF must beSome(3).await
    }

    "Count all elements" in new EnumeratorFixture {
      val countF = new PlayStreamIterator(integerEnumerator).count

      countF must be_==(5).await
    }

    "Count all elements with predicate" in new EnumeratorFixture {
      val countF = new PlayStreamIterator(integerEnumerator).count(_ % 2 == 0)

      countF must be_==(2).await
    }

    "Count all elements with predicate future" in new EnumeratorFixture {
      val countF = new PlayStreamIterator(integerEnumerator).countM(el => Future(el % 2 == 0))

      countF must be_==(2).await
    }

    "Batch elements into an iterator of equal sized lists." in new EnumeratorFixture {
      val batched = new PlayStreamIterator(integerEnumerator).grouped(2)

      batched.count must be_==(3).await
    }

    "map over elements" in new EnumeratorFixture {
      val mapped = new PlayStreamIterator(integerEnumerator).map(_.toString)
      val future = mapped.foldLeftM("") {
        case (stringSoFar, nextChar) => Future(stringSoFar + nextChar)
      }
      future must be_==("12345").await
    }

    "map over elements with a future" in new EnumeratorFixture {
      val mapped = new PlayStreamIterator(integerEnumerator).mapM(el => Future(el.toString))
      val future = mapped.foldLeftM("") {
        case (stringSoFar, nextChar) => Future(stringSoFar + nextChar)
      }
      future must be_==("12345").await
    }

    "do an operation for each element with for each" in new EnumeratorFixture {
      var sum = 0
      val future = new PlayStreamIterator(integerEnumerator).foreach(i => sum += i)

      Await.result(future, atMost = 5.seconds)
      sum must_== 15
    }

    "forall should return true when all elements satisfy predicate" in new EnumeratorFixture {
      val forAllF = new PlayStreamIterator(integerEnumerator).forall(i => Future(i < 10))

      forAllF must beTrue.await
    }

    "forall should return false when some elements fail predicate" in new EnumeratorFixture {
      val forAllF = new PlayStreamIterator(integerEnumerator).forall(i => Future(i % 2 == 0))

      forAllF must beFalse.await
    }

    "foldLeft" in new EnumeratorFixture {
      val sumFuture: Future[Int] = new PlayStreamIterator(integerEnumerator).foldLeft(0) {
        case (sumSoFar, nextElt) => sumSoFar + nextElt
      }

      sumFuture must be_==(15).await
    }

    "foldLeft" in new EnumeratorFixture {
      val sumFuture: Future[Int] = new PlayStreamIterator(integerEnumerator).foldLeftM(0) {
        case (sumSoFar, nextElt) => Future(sumSoFar + nextElt)
      }

      sumFuture must be_==(15).await
    }

    "take n elements" in new EnumeratorFixture {
      val iterator = new PlayStreamIterator(integerEnumerator)
      val takeTwo = iterator.take(2)
      takeTwo.count must be_==(2).await
    }

    "take elements while predicate is satisfied" in new EnumeratorFixture {
      val iterator = new PlayStreamIterator(integerEnumerator)
      val takeFirstThree = iterator.takeWhile(_ < 4)
      takeFirstThree.headOption must beSome(1).await
      takeFirstThree.count must be_==(2).await
    }

    "drop elements while predicate is satisfied" in new EnumeratorFixture {
      val iterator = new PlayStreamIterator(integerEnumerator)
      val dropFirstTwo = iterator.dropWhile(_ < 4)
      dropFirstTwo.headOption must beSome(4).await
    }

    "drop n elements" in new EnumeratorFixture {
      val iterator = new PlayStreamIterator(integerEnumerator)
      val takeTwo = iterator.drop(2)
      takeTwo.count must be_==(3).await
    }

    "flatten with list elements" in new EnumeratorListFixture {

      val flattenedSeq = Await.result(
        {
          val seqFuture =
            PlayStreamIterator[Seq[Int]](intListEnumerator)
              .flatten[Int]
              .take(sizeIntsInLists)
              .foldLeft(Seq.empty[Int])({ case (accum, x) => accum :+ x })
          seqFuture.map(_.size) must be_==(sizeIntsInLists).await
          seqFuture
        },
        Duration.Inf
      )

      val allEqualInSequence = {
        var i = 0
        PlayStreamIterator[Seq[Int]](intListEnumerator)
          .forall(seq =>
            Future(
              seq.forall(x => {
                val fromFlattenedSeq = flattenedSeq(i)
                i += 1
                fromFlattenedSeq == x
              })
            )
          )
      }

      allEqualInSequence must be_==(true).await
    }

    "flatMap with int elements, using Option" in new EnumeratorFixture {

      val evenOnly =
        PlayStreamIterator[Int](integerEnumerator)
          .flatMap(i =>
            travOnce2Enumerator(
              if (i % 2 == 0)
                Some(i)
              else
                None
            )
          )

      evenOnly.forall(i => Future(i % 2 == 0)) must be_==(true).await
    }
  }

}
