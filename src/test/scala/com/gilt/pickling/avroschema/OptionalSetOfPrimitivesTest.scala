package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class OptionalSetOfPrimitivesTest extends FunSuite with Assertions {
  test("Generate schema from a case class with a some set of int field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfInts.avsc") === fingerPrint(Schema(OptionSetOfInts(Some(Set(123))))))
    assert(fingerPrint("/avro/option-sets/OptionSetOfInts.avsc") === fingerPrint(Schema(classOf[OptionSetOfInts])))
    assert(fingerPrint("/avro/option-sets/OptionSetOfInts.avsc") === fingerPrint(Schema[OptionSetOfInts]))
  }

  test("Generate schema from a case class with a some set of long field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfLongs.avsc") === fingerPrint(Schema(OptionSetOfLongs(Some(Set(123L))))))
    assert(fingerPrint("/avro/option-sets/OptionSetOfLongs.avsc") === fingerPrint(Schema(classOf[OptionSetOfLongs])))
    assert(fingerPrint("/avro/option-sets/OptionSetOfLongs.avsc") === fingerPrint(Schema[OptionSetOfLongs]))
  }

  test("Generate schema from a case class with a some set of double field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfDoubles.avsc") === fingerPrint(Schema(OptionSetOfDoubles(Some(Set(123D))))))
    assert(fingerPrint("/avro/option-sets/OptionSetOfDoubles.avsc") === fingerPrint(Schema(classOf[OptionSetOfDoubles])))
    assert(fingerPrint("/avro/option-sets/OptionSetOfDoubles.avsc") === fingerPrint(Schema[OptionSetOfDoubles]))
  }

  test("Generate schema from a case class with a some set of float field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfFloats.avsc") === fingerPrint(Schema(OptionSetOfFloats(Some(Set(123F))))))
    assert(fingerPrint("/avro/option-sets/OptionSetOfFloats.avsc") === fingerPrint(Schema(classOf[OptionSetOfFloats])))
    assert(fingerPrint("/avro/option-sets/OptionSetOfFloats.avsc") === fingerPrint(Schema[OptionSetOfFloats]))
  }

  test("Generate schema from a case class with a some set of boolean field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfBooleans.avsc") === fingerPrint(Schema(OptionSetOfBooleans(Some(Set(false, true))))))
    assert(fingerPrint("/avro/option-sets/OptionSetOfBooleans.avsc") === fingerPrint(Schema(classOf[OptionSetOfBooleans])))
    assert(fingerPrint("/avro/option-sets/OptionSetOfBooleans.avsc") === fingerPrint(Schema[OptionSetOfBooleans]))
  }

  test("Generate schema from a case class with a some set of string field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfStrings.avsc") === fingerPrint(Schema(OptionSetOfStrings(Some(Set("abc"))))))
    assert(fingerPrint("/avro/option-sets/OptionSetOfStrings.avsc") === fingerPrint(Schema(classOf[OptionSetOfStrings])))
    assert(fingerPrint("/avro/option-sets/OptionSetOfStrings.avsc") === fingerPrint(Schema[OptionSetOfStrings]))
  }

  test("Generate schema from a case class with a some set of char field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfChars.avsc") === fingerPrint(Schema(OptionSetOfChars(Some(Set('a'))))))
    assert(fingerPrint("/avro/option-sets/OptionSetOfChars.avsc") === fingerPrint(Schema(classOf[OptionSetOfChars])))
    assert(fingerPrint("/avro/option-sets/OptionSetOfChars.avsc") === fingerPrint(Schema[OptionSetOfChars]))
  }

  test("Generate schema from a case class with a some set of byte field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfBytes.avsc") === fingerPrint(Schema(OptionSetOfBytes(Some(Set(1.toByte))))))
    assert(fingerPrint("/avro/option-sets/OptionSetOfBytes.avsc") === fingerPrint(Schema(classOf[OptionSetOfBytes])))
    assert(fingerPrint("/avro/option-sets/OptionSetOfBytes.avsc") === fingerPrint(Schema[OptionSetOfBytes]))
  }

  test("Generate schema from a case class with a some set of short field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfShorts.avsc") === fingerPrint(Schema(OptionSetOfShorts(Some(Set(1.toShort))))))
    assert(fingerPrint("/avro/option-sets/OptionSetOfShorts.avsc") === fingerPrint(Schema(classOf[OptionSetOfShorts])))
    assert(fingerPrint("/avro/option-sets/OptionSetOfShorts.avsc") === fingerPrint(Schema[OptionSetOfShorts]))
  }
}
