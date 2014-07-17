package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs.{ClassInnerObject, ObjInnerObject}

class OnlyCaseClassAsRootObjTests extends FunSuite with Assertions {

  test("Should not serialise an Object") {
    intercept[IllegalArgumentException] (Schema(ObjInnerObject))
  }

  test("Should not serialise an instance of a Class") {
    intercept[IllegalArgumentException] (Schema(new ClassInnerObject(1)))
    intercept[IllegalArgumentException] (Schema(classOf[ClassInnerObject]))
    intercept[IllegalArgumentException] (Schema[ClassInnerObject])
  }

  test("Should not serialise a List as a root") {
    intercept[IllegalArgumentException] (Schema(List(1)))
  }

  test("Should not serialise a Array as a root") {
    intercept[IllegalArgumentException] (Schema(Array(1)))
  }

  test("Should not serialise a Set as a root") {
    intercept[IllegalArgumentException] (Schema(Set(1)))
  }

  test("Should not serialise a Integer as a root") {
    intercept[IllegalArgumentException] (Schema(1))
    intercept[IllegalArgumentException] (Schema(classOf[Int]))
    intercept[IllegalArgumentException] (Schema[Int])
  }

  test("Should not serialise a Long as a root") {
    intercept[IllegalArgumentException] (Schema(1L))
    intercept[IllegalArgumentException] (Schema(classOf[Long]))
    intercept[IllegalArgumentException] (Schema[Long])
  }
  test("Should not serialise a Float as a root") {
    intercept[IllegalArgumentException] (Schema(1F))
    intercept[IllegalArgumentException] (Schema(classOf[Float]))
    intercept[IllegalArgumentException] (Schema[Float])
  }

  test("Should not serialise a Double as a root") {
    intercept[IllegalArgumentException] (Schema(1D))
    intercept[IllegalArgumentException] (Schema(classOf[Double]))
    intercept[IllegalArgumentException] (Schema[Double])
  }

  test("Should not serialise a Boolean as a root") {
    intercept[IllegalArgumentException] (Schema(true))
    intercept[IllegalArgumentException] (Schema(classOf[Boolean]))
    intercept[IllegalArgumentException] (Schema[Boolean])
  }

  test("Should not serialise a String as a root") {
    intercept[IllegalArgumentException] (Schema("abc"))
    intercept[IllegalArgumentException] (Schema(classOf[String]))
    intercept[IllegalArgumentException] (Schema[String])
  }

  test("Should not serialise a Byte as a root") {
    intercept[IllegalArgumentException] (Schema(1.toByte))
    intercept[IllegalArgumentException] (Schema(classOf[Byte]))
    intercept[IllegalArgumentException] (Schema[Byte])
  }

  test("Should not serialise a Short as a root") {
    intercept[IllegalArgumentException] (Schema(1.toShort))
    intercept[IllegalArgumentException] (Schema(classOf[Short]))
    intercept[IllegalArgumentException] (Schema[Short])
  }

  test("Should not serialise a Char as a root") {
    intercept[IllegalArgumentException] (Schema('c'))
    intercept[IllegalArgumentException] (Schema(classOf[Char]))
    intercept[IllegalArgumentException] (Schema[Char])
  }

  test("Should not serialise a Unit") {
    val someUnit: Unit = ()
    // a scala pickling error
    intercept[IllegalArgumentException] (Schema(someUnit))
    intercept[IllegalArgumentException] (Schema(classOf[Unit]))
    intercept[IllegalArgumentException] (Schema[Unit])
  }
}
