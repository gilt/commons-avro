package com.gilt.pickling.avro

import org.apache.avro.generic.GenericData
import org.apache.avro.Schema
import org.scalatest.{Assertions, FunSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import AvroPickleSingleFieldPrimitivesTest._
import scala.pickling._
import TestUtils._


object AvroPickleSingleFieldPrimitivesTest {
  case class SingleInt(id: Int)
  case class SingleLong(id: Long)
  case class SingleDouble(id: Double)
  case class SingleFloat(id: Float)
  case class SingleBoolean(id: Boolean)
  case class SingleString(id: String)
  case class SingleByte(id: Byte)
  case class SingleShort(id: Short)
  case class SingleChar(id: Char)
}

class AvroPickleSingleFieldPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  //Single Int
  test("Pickle a case class with a single int field") {
    forAll {
      (int: Int) =>
        val obj = new SingleInt(int)
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(int, "/avro/single/SingleInt.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single int field") {
    forAll {
      (int: Int) =>
        val bytes = generateSingleValueBytesFromAvro(int, "/avro/single/SingleInt.avsc")
        val obj: SingleInt = bytes.unpickle[SingleInt]
        assert(obj === new SingleInt(int))
    }
  }

  test("Round trip a case class with a single int field") {
    forAll {
      (int: Int) =>
        val obj = new SingleInt(int)
        val pckl = obj.pickle
        val hydratedObj: SingleInt = pckl.unpickle[SingleInt]
        assert(hydratedObj === obj)
    }
  }

  //Single Long
  test("Pickle a case class with a single long field") {
    forAll {
      (long: Long) =>
        val obj = new SingleLong(long)
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(long, "/avro/single/SingleLong.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single long field") {
    forAll {
      (long: Long) =>
        val bytes = generateSingleValueBytesFromAvro(long, "/avro/single/SingleLong.avsc")
        val obj: SingleLong = bytes.unpickle[SingleLong]
        assert(obj === new SingleLong(long))
    }
  }

  test("Round trip a case class with a single long field") {
    forAll {
      (long: Long) =>
      val obj = new SingleLong(long)
      val pckl = obj.pickle
      val hydratedObj: SingleLong = pckl.unpickle[SingleLong]
      assert(hydratedObj === obj)
    }
  }

  //Single Float
  test("Pickle a case class with a single float field") {
    val obj = new SingleFloat(0.123F)
    val pckl = obj.pickle
    assert(generateSingleValueBytesFromAvro(0.123F, "/avro/single/SingleFloat.avsc") === pckl.value)
  }

  test("Unpickle a case class with a single float field") {
    val bytes = generateSingleValueBytesFromAvro(0.123F, "/avro/single/SingleFloat.avsc")
    val obj: SingleFloat = bytes.unpickle[SingleFloat]
    assert(obj === new SingleFloat(0.123F))
  }

  test("Round trip a case class with a single float field") {
    val obj = new SingleFloat(0.123F)
    val pckl = obj.pickle
    val hydratedObj: SingleFloat = pckl.unpickle[SingleFloat]
    assert(hydratedObj === obj)
  }

  //Single Double
  test("Pickle a case class with a single double field") {
    val obj = new SingleDouble(0.123)
    val pckl = obj.pickle
    assert(generateSingleValueBytesFromAvro(0.123D, "/avro/single/SingleDouble.avsc") === pckl.value)
  }

  test("Unpickle a case class with a single double field") {
    val bytes = generateSingleValueBytesFromAvro(0.123, "/avro/single/SingleDouble.avsc")
    val obj: SingleDouble = bytes.unpickle[SingleDouble]
    assert(obj === new SingleDouble(0.123))
  }

  test("Round trip a case class with a single double field") {
    val obj = new SingleDouble(0.123)
    val pckl = obj.pickle
    val hydratedObj: SingleDouble = pckl.unpickle[SingleDouble]
    assert(hydratedObj === obj)
  }

  //Single Boolean
  test("Pickle a case class with a single boolean field") {
    val obj = new SingleBoolean(true)
    val pckl = obj.pickle
    assert(generateSingleValueBytesFromAvro(true, "/avro/single/SingleBoolean.avsc") === pckl.value)
  }

  test("Unpickle a case class with a single boolean field") {
    val bytes = generateSingleValueBytesFromAvro(true, "/avro/single/SingleBoolean.avsc")
    val obj: SingleBoolean = bytes.unpickle[SingleBoolean]
    assert(obj === new SingleBoolean(true))
  }

  test("Round trip a case class with a single boolean field") {
    val obj = new SingleBoolean(true)
    val pckl = obj.pickle
    val hydratedObj: SingleBoolean = pckl.unpickle[SingleBoolean]
    assert(hydratedObj === obj)
  }

  //Single String
  test("Pickle a case class with a single string field") {
    val obj = new SingleString("some crazy id")
    val pckl = obj.pickle
    assert(generateSingleValueBytesFromAvro("some crazy id", "/avro/single/SingleString.avsc") === pckl.value)
  }

  test("Unpickle a case class with a single string field") {
    val bytes = generateSingleValueBytesFromAvro("some crazy id", "/avro/single/SingleString.avsc")
    val obj: SingleString = bytes.unpickle[SingleString]
    assert(obj === new SingleString("some crazy id"))
  }

  test("Round trip a case class with a single string field") {
    val obj = new SingleString("some crazy id")
    val pckl = obj.pickle
    val hydratedObj: SingleString = pckl.unpickle[SingleString]
    assert(hydratedObj === obj)
  }

  //Single Byte
  test("Pickle a case class with a single byte field") {
    val obj = new SingleByte(1.toByte)
    val pckl = obj.pickle
    assert(generateSingleValueBytesFromAvro(1.toByte, "/avro/single/SingleByte.avsc") === pckl.value)
  }

  test("Unpickle a case class with a single byte field") {
    val bytes = generateSingleValueBytesFromAvro(1.toByte, "/avro/single/SingleByte.avsc")
    val obj: SingleByte = bytes.unpickle[SingleByte]
    assert(obj === new SingleByte(1.toByte))
  }

  test("Round trip a case class with a single byte field") {
    val obj = new SingleByte(1.toByte)
    val pckl = obj.pickle
    val hydratedObj: SingleByte = pckl.unpickle[SingleByte]
    assert(hydratedObj === obj)
  }

  //Single Short
  test("Pickle a case class with a single short field") {
    val obj = new SingleShort(2)
    val pckl = obj.pickle
    assert(generateSingleValueBytesFromAvro(2, "/avro/single/SingleShort.avsc") === pckl.value)
  }

  test("Unpickle a case class with a single short field") {
    val bytes = generateSingleValueBytesFromAvro(2.toShort, "/avro/single/SingleShort.avsc")
    val obj: SingleShort = bytes.unpickle[SingleShort]
    assert(obj === new SingleShort(2.toShort))
  }

  test("Round trip a case class with a single short field") {
    val obj = new SingleShort(2.toShort)
    val pckl = obj.pickle
    val hydratedObj: SingleShort = pckl.unpickle[SingleShort]
    assert(hydratedObj === obj)
  }

  //Single Char
  test("Pickle a case class with a single char field") {
    val obj = new SingleChar('c')
    val pckl = obj.pickle
    assert(generateSingleValueBytesFromAvro('c'.toInt, "/avro/single/SingleChar.avsc") === pckl.value)
  }

  test("Unpickle a case class with a single char field") {
    val bytes = generateSingleValueBytesFromAvro('c'.toInt, "/avro/single/SingleChar.avsc")
    val obj: SingleChar = bytes.unpickle[SingleChar]
    assert(obj === new SingleChar('c'))
  }

  test("Round trip a case class with a single char field") {
    val obj = new SingleChar('c')
    val pckl = obj.pickle
    val hydratedObj: SingleChar = pckl.unpickle[SingleChar]
    assert(hydratedObj === obj)
  }

  private def generateSingleValueBytesFromAvro(value: Any, schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("id", value)
    convertToBytes(schema, record)
  }
}
