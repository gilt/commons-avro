package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import com.gilt.pickling.TestObjs._

object AssortedCollectionTests {

  implicit val arbVc = Arbitrary {
    for (ints <- Gen.containerOf[Vector, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield VectorContainer(ints)
  }

  implicit val arbSc = Arbitrary {
    for (ints <- Gen.containerOf[Seq, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield SeqContainer(ints)
  }
}

class AssortedCollectionTests extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  import AssortedCollectionTests._
  import scala.pickling._

  test("vector roundtrip") {
    forAll {
      (obj: VectorContainer) =>
        assert(obj === obj.pickle.unpickle[VectorContainer])
    }
  }

  test("seq roundtrip") {
    forAll {
      (obj: SeqContainer) =>
        assert(obj === obj.pickle.unpickle[SeqContainer])
    }
  }
}
