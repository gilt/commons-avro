package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class SingleOptionalFieldTest extends FunSuite with Assertions {

  //Some option
  test("Generate schema from a case class with a single optional int field") {
    assert(fingerPrint("/avro/option/SingleOptionInt.avsc") === fingerPrint(SingleOptionInt(Some(123)).pickle.value))
  }

  test("Generate schema from a case class with a single optional long field") {
    assert(fingerPrint("/avro/option/SingleOptionLong.avsc") === fingerPrint(SingleOptionLong(Some(123)).pickle.value))
  }

  test("Generate schema from a case class with a single optional double field") {
    assert(fingerPrint("/avro/option/SingleOptionDouble.avsc") === fingerPrint(SingleOptionDouble(Some(123)).pickle.value))
  }

  test("Generate schema from a case class with a single optional float field") {
    assert(fingerPrint("/avro/option/SingleOptionFloat.avsc") === fingerPrint(SingleOptionFloat(Some(123)).pickle.value))
  }

  test("Generate schema from a case class with a single optional boolean field") {
    assert(fingerPrint("/avro/option/SingleOptionBoolean.avsc") === fingerPrint(SingleOptionBoolean(Some(true)).pickle.value))
  }

  test("Generate schema from a case class with a single optional string field") {
    assert(fingerPrint("/avro/option/SingleOptionString.avsc") === fingerPrint(SingleOptionString(Some("abc")).pickle.value))
  }

  test("Generate schema from a case class with a single optional char field") {
    assert(fingerPrint("/avro/option/SingleOptionChar.avsc") === fingerPrint(SingleOptionChar(Some('a')).pickle.value))
  }

  test("Generate schema from a case class with a single optional byte field") {
    assert(fingerPrint("/avro/option/SingleOptionByte.avsc") === fingerPrint(SingleOptionByte(Some(1.toByte)).pickle.value))
  }

  test("Generate schema from a case class with a single optional short field") {
    assert(fingerPrint("/avro/option/SingleOptionShort.avsc") === fingerPrint(SingleOptionShort(Some(1.toShort)).pickle.value))
  }

  //None option
  test("Generate schema from a case class with a single none optional int field") {
    assert(fingerPrint("/avro/option/SingleOptionInt.avsc") === fingerPrint(SingleOptionInt(None).pickle.value))
  }

  test("Generate schema from a case class with a single none optional long field") {
    assert(fingerPrint("/avro/option/SingleOptionLong.avsc") === fingerPrint(SingleOptionLong(None).pickle.value))
  }

  test("Generate schema from a case class with a single none optional double field") {
    assert(fingerPrint("/avro/option/SingleOptionDouble.avsc") === fingerPrint(SingleOptionDouble(None).pickle.value))
  }

  test("Generate schema from a case class with a single none optional float field") {
    assert(fingerPrint("/avro/option/SingleOptionFloat.avsc") === fingerPrint(SingleOptionFloat(None).pickle.value))
  }

  test("Generate schema from a case class with a single none optional boolean field") {
    assert(fingerPrint("/avro/option/SingleOptionBoolean.avsc") === fingerPrint(SingleOptionBoolean(None).pickle.value))
  }

  test("Generate schema from a case class with a single none optional string field") {
    assert(fingerPrint("/avro/option/SingleOptionString.avsc") === fingerPrint(SingleOptionString(None).pickle.value))
  }

  test("Generate schema from a case class with a single none optional char field") {
    assert(fingerPrint("/avro/option/SingleOptionChar.avsc") === fingerPrint(SingleOptionChar(None).pickle.value))
  }

  test("Generate schema from a case class with a single none optional byte field") {
    assert(fingerPrint("/avro/option/SingleOptionByte.avsc") === fingerPrint(SingleOptionByte(None).pickle.value))
  }

  test("Generate schema from a case class with a single none optional short field") {
    assert(fingerPrint("/avro/option/SingleOptionShort.avsc") === fingerPrint(SingleOptionShort(None).pickle.value))
  }
}
