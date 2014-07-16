package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class SingleFieldTest extends FunSuite with Assertions {

  test("Generate schema from a case class with a single int field") {
    assert(fingerPrint("/avro/single/SingleInt.avsc") === fingerPrint(SingleInt(123).pickle.value))
    assert(fingerPrint("/avro/single/SingleInt.avsc") === fingerPrint(Schema(SingleInt(123))))
    assert(fingerPrint("/avro/single/SingleInt.avsc") === fingerPrint(Schema(classOf[SingleInt])))
    assert(fingerPrint("/avro/single/SingleInt.avsc") === fingerPrint(Schema[SingleInt]))
  }

  test("Generate schema from a case class with a single long field") {
    assert(fingerPrint("/avro/single/SingleLong.avsc") === fingerPrint(SingleLong(123).pickle.value))
    assert(fingerPrint("/avro/single/SingleLong.avsc") === fingerPrint(Schema(SingleLong(123))))
    assert(fingerPrint("/avro/single/SingleLong.avsc") === fingerPrint(Schema(classOf[SingleLong])))
    assert(fingerPrint("/avro/single/SingleLong.avsc") === fingerPrint(Schema[SingleLong]))
  }

  test("Generate schema from a case class with a single double field") {
    assert(fingerPrint("/avro/single/SingleDouble.avsc") === fingerPrint(SingleDouble(123).pickle.value))
    assert(fingerPrint("/avro/single/SingleDouble.avsc") === fingerPrint(Schema(SingleDouble(123))))
    assert(fingerPrint("/avro/single/SingleDouble.avsc") === fingerPrint(Schema(classOf[SingleDouble])))
    assert(fingerPrint("/avro/single/SingleDouble.avsc") === fingerPrint(Schema[SingleDouble]))
  }

  test("Generate schema from a case class with a single float field") {
    assert(fingerPrint("/avro/single/SingleFloat.avsc") === fingerPrint(SingleFloat(123).pickle.value))
    assert(fingerPrint("/avro/single/SingleFloat.avsc") === fingerPrint(Schema(SingleFloat(123))))
    assert(fingerPrint("/avro/single/SingleFloat.avsc") === fingerPrint(Schema(classOf[SingleFloat])))
    assert(fingerPrint("/avro/single/SingleFloat.avsc") === fingerPrint(Schema[SingleFloat]))
  }

  test("Generate schema from a case class with a single boolean field") {
    assert(fingerPrint("/avro/single/SingleBoolean.avsc") === fingerPrint(SingleBoolean(id = true).pickle.value))
    assert(fingerPrint("/avro/single/SingleBoolean.avsc") === fingerPrint(Schema(SingleBoolean(id = true))))
    assert(fingerPrint("/avro/single/SingleBoolean.avsc") === fingerPrint(Schema(classOf[SingleBoolean])))
    assert(fingerPrint("/avro/single/SingleBoolean.avsc") === fingerPrint(Schema[SingleBoolean]))
  }

  test("Generate schema from a case class with a single string field") {
    assert(fingerPrint("/avro/single/SingleString.avsc") === fingerPrint(SingleString("abc").pickle.value))
    assert(fingerPrint("/avro/single/SingleString.avsc") === fingerPrint(Schema(SingleString("abc"))))
    assert(fingerPrint("/avro/single/SingleString.avsc") === fingerPrint(Schema(classOf[SingleString])))
    assert(fingerPrint("/avro/single/SingleString.avsc") === fingerPrint(Schema[SingleString]))
  }

  test("Generate schema from a case class with a single char field") {
    assert(fingerPrint("/avro/single/SingleChar.avsc") === fingerPrint(SingleChar('a').pickle.value))
    assert(fingerPrint("/avro/single/SingleChar.avsc") === fingerPrint(Schema(SingleChar('a'))))
    assert(fingerPrint("/avro/single/SingleChar.avsc") === fingerPrint(Schema(classOf[SingleChar])))
    assert(fingerPrint("/avro/single/SingleChar.avsc") === fingerPrint(Schema[SingleChar]))
  }

  test("Generate schema from a case class with a single byte field") {
    assert(fingerPrint("/avro/single/SingleByte.avsc") === fingerPrint(SingleByte(1.toByte).pickle.value))
    assert(fingerPrint("/avro/single/SingleByte.avsc") === fingerPrint(Schema(SingleByte(1.toByte))))
    assert(fingerPrint("/avro/single/SingleByte.avsc") === fingerPrint(Schema(classOf[SingleByte])))
    assert(fingerPrint("/avro/single/SingleByte.avsc") === fingerPrint(Schema[SingleByte]))
  }

  test("Generate schema from a case class with a single short field") {
    assert(fingerPrint("/avro/single/SingleShort.avsc") === fingerPrint(SingleShort(1.toShort).pickle.value))
    assert(fingerPrint("/avro/single/SingleShort.avsc") === fingerPrint(Schema(SingleShort(1.toShort))))
    assert(fingerPrint("/avro/single/SingleShort.avsc") === fingerPrint(Schema(classOf[SingleShort])))
    assert(fingerPrint("/avro/single/SingleShort.avsc") === fingerPrint(Schema[SingleShort]))
  }

}
