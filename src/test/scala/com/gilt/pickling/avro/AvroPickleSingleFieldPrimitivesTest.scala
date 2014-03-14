package com.gilt.pickling.avro

import org.apache.avro.generic.GenericData
import org.apache.avro.Schema
import org.scalatest.{Assertions, FunSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import AvroPickleSingleFieldPrimitivesTest._
import scala.pickling._
import avro._
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
    forAll {
      (float: Float) =>
        val obj = new SingleFloat(float)
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(float, "/avro/single/SingleFloat.avsc") === pckl.value)

    }
  }

  test("Unpickle a case class with a single float field") {
    forAll {
      (float: Float) =>
        val bytes = generateSingleValueBytesFromAvro(float, "/avro/single/SingleFloat.avsc")
        val obj: SingleFloat = bytes.unpickle[SingleFloat]
        assert(obj === new SingleFloat(float))
    }
  }

  test("Round trip a case class with a single float field") {
    forAll {
      (float: Float) =>
        val obj = new SingleFloat(float)
        val pckl = obj.pickle
        val hydratedObj: SingleFloat = pckl.unpickle[SingleFloat]
        assert(hydratedObj === obj)
    }
  }

  //Single Double
  test("Pickle a case class with a single double field") {
    forAll {
      (double: Double) =>
        val obj = new SingleDouble(double)
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(double, "/avro/single/SingleDouble.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single double field") {
    forAll {
      (double: Double) =>
        val bytes = generateSingleValueBytesFromAvro(double, "/avro/single/SingleDouble.avsc")
        val obj: SingleDouble = bytes.unpickle[SingleDouble]
        assert(obj === new SingleDouble(double))
    }
  }

  test("Round trip a case class with a single double field") {
    forAll {
      (double: Double) =>
        val obj = new SingleDouble(double)
        val pckl = obj.pickle
        val hydratedObj: SingleDouble = pckl.unpickle[SingleDouble]
        assert(hydratedObj === obj)
    }
  }

  //Single Boolean
  test("Pickle a case class with a single boolean field") {
    forAll {
      (boolean: Boolean) =>
        val obj = new SingleBoolean(boolean)
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(boolean, "/avro/single/SingleBoolean.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single boolean field") {
    forAll {
      (boolean: Boolean) =>
        val bytes = generateSingleValueBytesFromAvro(boolean, "/avro/single/SingleBoolean.avsc")
        val obj: SingleBoolean = bytes.unpickle[SingleBoolean]
        assert(obj === new SingleBoolean(boolean))
    }
  }

  test("Round trip a case class with a single boolean field") {
    forAll {
      (boolean: Boolean) =>
        val obj = new SingleBoolean(boolean)
        val pckl = obj.pickle
        val hydratedObj: SingleBoolean = pckl.unpickle[SingleBoolean]
        assert(hydratedObj === obj)
    }
  }

  //Single String
  test("Pickle a case class with a single string field") {
    forAll {
      (string: String) =>
        val obj = new SingleString(string)
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(string, "/avro/single/SingleString.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single string field") {
    forAll {
      (string: String) =>
        val bytes = generateSingleValueBytesFromAvro(string, "/avro/single/SingleString.avsc")
        val obj: SingleString = bytes.unpickle[SingleString]
        assert(obj === new SingleString(string))
    }
  }

  test("Round trip a case class with a single string field") {
    forAll {
      (string: String) =>
        val obj = new SingleString(string)
        val pckl = obj.pickle
        val hydratedObj: SingleString = pckl.unpickle[SingleString]
        assert(hydratedObj === obj)
    }
  }

  //Single Byte
  test("Pickle a case class with a single byte field") {
    forAll {
      (byte: Byte) =>
        val obj = new SingleByte(byte)
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(byte, "/avro/single/SingleByte.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single byte field") {
    forAll {
      (byte: Byte) =>
        val bytes = generateSingleValueBytesFromAvro(byte, "/avro/single/SingleByte.avsc")
        val obj: SingleByte = bytes.unpickle[SingleByte]
        assert(obj === new SingleByte(byte))
    }
  }

  test("Round trip a case class with a single byte field") {
    forAll {
      (byte: Byte) =>
        val obj = new SingleByte(byte)
        val pckl = obj.pickle
        val hydratedObj: SingleByte = pckl.unpickle[SingleByte]
        assert(hydratedObj === obj)
    }
  }

  //Single Short
  test("Pickle a case class with a single short field") {
    forAll {
      (short: Short) =>
        val obj = new SingleShort(short)
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(short, "/avro/single/SingleShort.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single short field") {
    forAll {
      (short: Short) =>
        val bytes = generateSingleValueBytesFromAvro(short, "/avro/single/SingleShort.avsc")
        val obj: SingleShort = bytes.unpickle[SingleShort]
        assert(obj === new SingleShort(short))
    }
  }

  test("Round trip a case class with a single short field") {
    forAll {
      (short: Short) =>
        val obj = new SingleShort(short)
        val pckl = obj.pickle
        val hydratedObj: SingleShort = pckl.unpickle[SingleShort]
        assert(hydratedObj === obj)
    }
  }

  //Single Char
  test("Pickle a case class with a single char field") {
    forAll {
      (char: Char) =>
        val obj = new SingleChar(char)
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(char.toInt, "/avro/single/SingleChar.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single char field") {
    forAll {
      (char: Char) =>
        val bytes = generateSingleValueBytesFromAvro(char.toInt, "/avro/single/SingleChar.avsc")
        val obj: SingleChar = bytes.unpickle[SingleChar]
        assert(obj === new SingleChar(char))
    }
  }

  test("Round trip a case class with a single char field") {
    forAll {
      (char: Char) =>
        val obj = new SingleChar(char)
        val pckl = obj.pickle
        val hydratedObj: SingleChar = pckl.unpickle[SingleChar]
        assert(hydratedObj === obj)
    }
  }

  private def generateSingleValueBytesFromAvro(value: Any, schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("id", value)
    convertToBytes(schema, record)
  }
}
