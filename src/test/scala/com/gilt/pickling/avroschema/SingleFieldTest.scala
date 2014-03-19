package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class SingleFieldTest extends FunSuite with Assertions {

  test("Generate schema from a case class with a single int field") {
    assert(fingerPrint("/avro/single/SingleInt.avsc") === fingerPrint(SingleInt(123).pickle.value))
  }

  test("Generate schema from a case class with a single long field") {
    assert(fingerPrint("/avro/single/SingleLong.avsc") === fingerPrint(SingleLong(123).pickle.value))
  }

  test("Generate schema from a case class with a single double field") {
    assert(fingerPrint("/avro/single/SingleDouble.avsc") === fingerPrint(SingleDouble(123).pickle.value))
  }

  test("Generate schema from a case class with a single float field") {
    assert(fingerPrint("/avro/single/SingleFloat.avsc") === fingerPrint(SingleFloat(123).pickle.value))
  }

  test("Generate schema from a case class with a single boolean field") {
    assert(fingerPrint("/avro/single/SingleBoolean.avsc") === fingerPrint(SingleBoolean(id = true).pickle.value))
  }

  test("Generate schema from a case class with a single string field") {
    assert(fingerPrint("/avro/single/SingleString.avsc") === fingerPrint(SingleString("abc").pickle.value))
  }

  test("Generate schema from a case class with a single char field") {
    assert(fingerPrint("/avro/single/SingleChar.avsc") === fingerPrint(SingleChar('a').pickle.value))
  }

  test("Generate schema from a case class with a single byte field") {
    assert(fingerPrint("/avro/single/SingleByte.avsc") === fingerPrint(SingleByte(1.toByte).pickle.value))
  }

  test("Generate schema from a case class with a single short field") {
    assert(fingerPrint("/avro/single/SingleShort.avsc") === fingerPrint(SingleShort(1.toShort).pickle.value))
  }

}
