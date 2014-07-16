package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class SingleOptionalFieldTest extends FunSuite with Assertions {

  //Some option
  test("Generate schema from a case class with a single optional int field") {
    assert(fingerPrint("/avro/option/SingleOptionInt.avsc") === fingerPrint(SingleOptionInt(Some(123)).pickle.value))
    assert(fingerPrint("/avro/option/SingleOptionInt.avsc") === fingerPrint(Schema(SingleOptionInt(Some(123)))))
    assert(fingerPrint("/avro/option/SingleOptionInt.avsc") === fingerPrint(Schema(classOf[SingleOptionInt])))
    assert(fingerPrint("/avro/option/SingleOptionInt.avsc") === fingerPrint(Schema[SingleOptionInt]))
  }

  test("Generate schema from a case class with a single optional long field") {
    assert(fingerPrint("/avro/option/SingleOptionLong.avsc") === fingerPrint(SingleOptionLong(Some(123)).pickle.value))
    assert(fingerPrint("/avro/option/SingleOptionLong.avsc") === fingerPrint(Schema(SingleOptionLong(Some(123)))))
    assert(fingerPrint("/avro/option/SingleOptionLong.avsc") === fingerPrint(Schema(classOf[SingleOptionLong])))
    assert(fingerPrint("/avro/option/SingleOptionLong.avsc") === fingerPrint(Schema[SingleOptionLong]))
  }

  test("Generate schema from a case class with a single optional double field") {
    assert(fingerPrint("/avro/option/SingleOptionDouble.avsc") === fingerPrint(SingleOptionDouble(Some(123)).pickle.value))
    assert(fingerPrint("/avro/option/SingleOptionDouble.avsc") === fingerPrint(Schema(SingleOptionDouble(Some(123)))))
    assert(fingerPrint("/avro/option/SingleOptionDouble.avsc") === fingerPrint(Schema(classOf[SingleOptionDouble])))
    assert(fingerPrint("/avro/option/SingleOptionDouble.avsc") === fingerPrint(Schema[SingleOptionDouble]))
  }

  test("Generate schema from a case class with a single optional float field") {
    assert(fingerPrint("/avro/option/SingleOptionFloat.avsc") === fingerPrint(SingleOptionFloat(Some(123)).pickle.value))
    assert(fingerPrint("/avro/option/SingleOptionFloat.avsc") === fingerPrint(Schema(SingleOptionFloat(Some(123)))))
    assert(fingerPrint("/avro/option/SingleOptionFloat.avsc") === fingerPrint(Schema(classOf[SingleOptionFloat])))
    assert(fingerPrint("/avro/option/SingleOptionFloat.avsc") === fingerPrint(Schema[SingleOptionFloat]))
  }

  test("Generate schema from a case class with a single optional boolean field") {
    assert(fingerPrint("/avro/option/SingleOptionBoolean.avsc") === fingerPrint(SingleOptionBoolean(Some(true)).pickle.value))
    assert(fingerPrint("/avro/option/SingleOptionBoolean.avsc") === fingerPrint(Schema(SingleOptionBoolean(Some(true)))))
    assert(fingerPrint("/avro/option/SingleOptionBoolean.avsc") === fingerPrint(Schema(classOf[SingleOptionBoolean])))
    assert(fingerPrint("/avro/option/SingleOptionBoolean.avsc") === fingerPrint(Schema[SingleOptionBoolean]))
  }

  test("Generate schema from a case class with a single optional string field") {
    assert(fingerPrint("/avro/option/SingleOptionString.avsc") === fingerPrint(SingleOptionString(Some("abc")).pickle.value))
    assert(fingerPrint("/avro/option/SingleOptionString.avsc") === fingerPrint(Schema(SingleOptionString(Some("abc")))))
    assert(fingerPrint("/avro/option/SingleOptionString.avsc") === fingerPrint(Schema(classOf[SingleOptionString])))
    assert(fingerPrint("/avro/option/SingleOptionString.avsc") === fingerPrint(Schema[SingleOptionString]))
  }

  test("Generate schema from a case class with a single optional char field") {
    assert(fingerPrint("/avro/option/SingleOptionChar.avsc") === fingerPrint(SingleOptionChar(Some('a')).pickle.value))
    assert(fingerPrint("/avro/option/SingleOptionChar.avsc") === fingerPrint(Schema(SingleOptionChar(Some('a')))))
    assert(fingerPrint("/avro/option/SingleOptionChar.avsc") === fingerPrint(Schema(classOf[SingleOptionChar])))
    assert(fingerPrint("/avro/option/SingleOptionChar.avsc") === fingerPrint(Schema[SingleOptionChar]))
  }

  test("Generate schema from a case class with a single optional byte field") {
    assert(fingerPrint("/avro/option/SingleOptionByte.avsc") === fingerPrint(SingleOptionByte(Some(1.toByte)).pickle.value))
    assert(fingerPrint("/avro/option/SingleOptionByte.avsc") === fingerPrint(Schema(SingleOptionByte(Some(1.toByte)))))
    assert(fingerPrint("/avro/option/SingleOptionByte.avsc") === fingerPrint(Schema(classOf[SingleOptionByte])))
    assert(fingerPrint("/avro/option/SingleOptionByte.avsc") === fingerPrint(Schema[SingleOptionByte]))
  }

  test("Generate schema from a case class with a single optional short field") {
    assert(fingerPrint("/avro/option/SingleOptionShort.avsc") === fingerPrint(SingleOptionShort(Some(1.toShort)).pickle.value))
    assert(fingerPrint("/avro/option/SingleOptionShort.avsc") === fingerPrint(Schema(SingleOptionShort(Some(1.toShort)))))
    assert(fingerPrint("/avro/option/SingleOptionShort.avsc") === fingerPrint(Schema(classOf[SingleOptionShort])))
    assert(fingerPrint("/avro/option/SingleOptionShort.avsc") === fingerPrint(Schema[SingleOptionShort]))
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
