package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import com.gilt.pickling.TestObjs.{ObjInnerObject, ClassInnerObject}

class OnlyCaseClassAsRootObjTests extends FunSuite with Assertions {

  test("Should not serialise an Object") {
    intercept[PicklingException] {
      ObjInnerObject.pickle
    }
  }

  test("Should not serialise an instance of a Class") {
    intercept[PicklingException] {
      new ClassInnerObject(1).pickle
    }
  }

  test("Should not deserialise to an instance of a Class") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[ClassInnerObject]
    }
  }

  test("Should not serialise a List as a root") {
    intercept[PicklingException] {
      List(1).pickle
    }
  }

  test("Should not deserialise a List as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[List[Int]]
    }
  }

  test("Should not serialise a Array as a root") {
    intercept[PicklingException] {
      Array(1).pickle
    }
  }

  test("Should not deserialise a Array as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Array[Int]]
    }
  }

  test("Should not serialise a Set as a root") {
    intercept[PicklingException] {
      Set(1).pickle
    }
  }

  test("Should not deserialise a Set as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Set[Int]]
    }

  }

  test("Should not serialise a Integer as a root") {
    intercept[PicklingException] {
      1.pickle
    }
  }

  test("Should not deserialise a Integer as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Integer]
    }
  }

  test("Should not serialise a Long as a root") {
    intercept[PicklingException] {
      1L.pickle
    }
  }

  test("Should not deserialise a Long as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Long]
    }
  }

  test("Should not serialise a Float as a root") {
    intercept[PicklingException] {
      1F.pickle
    }
  }

  test("Should not deserialise a Float as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Float]
    }
  }

  test("Should not serialise a Double as a root") {
    intercept[PicklingException] {
      1D.pickle
    }
  }

  test("Should not deserialise a Double as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Double]
    }
  }

  test("Should not serialise a Boolean as a root") {
    intercept[PicklingException] {
      true.pickle
    }
  }

  test("Should not deserialise a Boolean as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Boolean]
    }
  }

  test("Should not serialise a String as a root") {
    intercept[PicklingException] {
      "abc".pickle
    }
  }

  test("Should not deserialise a String as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[String]
    }
  }

  test("Should not serialise a Byte as a root") {
    intercept[PicklingException] {
      1.toByte.pickle
    }
  }

  test("Should not deserialise a Byte as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Byte]
    }
  }

  test("Should not serialise a Short as a root") {
    intercept[PicklingException] {
      1.toShort.pickle
    }
  }

  test("Should not deserialise a Short as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Short]
    }
  }

  test("Should not serialise a Char as a root") {
    intercept[PicklingException] {
      'c'.toShort.pickle
    }
  }

  test("Should not deserialise a Char as a root") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Char]
    }
  }

  test("Should not serialise a Unit") {
    val someUnit: Unit = {}
    // a scala pickling error
    intercept[NotImplementedError] {
      someUnit.pickle
    }
  }

  test("Should not deserialise a Unit") {
    intercept[PicklingException] {
      Array[Byte](1, 2, 3).unpickle[Unit]
    }
  }
}
