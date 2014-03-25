package com.gilt.pickling.avro

import org.apache.avro.generic.GenericData
import org.apache.avro.Schema
import org.scalatest.{Assertions, FunSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.pickling._
import com.gilt.pickling.TestUtils
import TestUtils._
import org.scalacheck.{Gen, Arbitrary}
import com.gilt.pickling.TestObjs._

object SingleFieldTest {
  implicit val arbSingleInt = Arbitrary(for (num <- Gen.choose(Int.MinValue, Int.MaxValue)) yield SingleInt(num))
  implicit val arbSingleLong = Arbitrary(for (num <- Gen.choose(Long.MinValue, Long.MaxValue)) yield SingleLong(num))
  implicit val arbSingleDouble = Arbitrary(for (num <- Gen.choose(Double.MinValue / 2, Double.MaxValue / 2)) yield SingleDouble(num))
  implicit val arbSingleFloat = Arbitrary(for (num <- Gen.choose(Float.MinValue, Float.MaxValue)) yield SingleFloat(num))
  implicit val arbSingleBoolean = Arbitrary(for (num <- Gen.oneOf(true, false)) yield SingleBoolean(num))
  implicit val arbSingleString = Arbitrary(for (num <- Gen.alphaStr) yield SingleString(num))
  implicit val arbSingleByte = Arbitrary(for (num <- Gen.choose(Byte.MinValue, Byte.MaxValue)) yield SingleByte(num))
  implicit val arbSingleShort = Arbitrary(for (num <- Gen.choose(Short.MinValue, Short.MaxValue)) yield SingleShort(num))
  implicit val arbSingleChar = Arbitrary(for (num <- Gen.choose(Char.MinValue, Char.MaxValue)) yield SingleChar(num))
}

class SingleFieldTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  import SingleFieldTest._

  //Single Int
  test("Pickle a case class with a single int field") {
    forAll {
      (obj: SingleInt) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/single/SingleInt.avsc") === pckl.value)
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
      (obj: SingleInt) =>
        val pckl = obj.pickle
        val hydratedObj: SingleInt = pckl.unpickle[SingleInt]
        assert(hydratedObj === obj)
    }
  }

  //Single Long
  test("Pickle a case class with a single long field") {
    forAll {
      (obj: SingleLong) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/single/SingleLong.avsc") === pckl.value)
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
      (obj: SingleLong) =>
        val pckl = obj.pickle
        val hydratedObj: SingleLong = pckl.unpickle[SingleLong]
        assert(hydratedObj === obj)
    }
  }

  //Single Float
  test("Pickle a case class with a single float field") {
    forAll {
      (obj: SingleFloat) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/single/SingleFloat.avsc") === pckl.value)

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
      (obj: SingleFloat) =>
        val pckl = obj.pickle
        val hydratedObj: SingleFloat = pckl.unpickle[SingleFloat]
        assert(hydratedObj === obj)
    }
  }

  //Single Double
  test("Pickle a case class with a single double field") {
    forAll {
      (obj: SingleDouble) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/single/SingleDouble.avsc") === pckl.value)
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
      (obj: SingleDouble) =>
        val pckl = obj.pickle
        val hydratedObj: SingleDouble = pckl.unpickle[SingleDouble]
        assert(hydratedObj === obj)
    }
  }

  //Single Boolean
  test("Pickle a case class with a single boolean field") {
    forAll {
      (obj: SingleBoolean) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/single/SingleBoolean.avsc") === pckl.value)
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
      (obj: SingleBoolean) =>
        val pckl = obj.pickle
        val hydratedObj: SingleBoolean = pckl.unpickle[SingleBoolean]
        assert(hydratedObj === obj)
    }
  }

  //Single String
  test("Pickle a case class with a single string field") {
    forAll {
      (obj: SingleString) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/single/SingleString.avsc") === pckl.value)
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
      (obj: SingleString) =>
        val pckl = obj.pickle
        val hydratedObj: SingleString = pckl.unpickle[SingleString]
        assert(hydratedObj === obj)
    }
  }

  //Single Byte
  test("Pickle a case class with a single byte field") {
    forAll {
      (obj: SingleByte) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/single/SingleByte.avsc") === pckl.value)
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
      (obj: SingleByte) =>
        val pckl = obj.pickle
        val hydratedObj: SingleByte = pckl.unpickle[SingleByte]
        assert(hydratedObj === obj)
    }
  }

  //Single Short
  test("Pickle a case class with a single short field") {
    forAll {
      (obj: SingleShort) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/single/SingleShort.avsc") === pckl.value)
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
      (obj: SingleShort) =>
        val pckl = obj.pickle
        val hydratedObj: SingleShort = pckl.unpickle[SingleShort]
        assert(hydratedObj === obj)
    }
  }

  //Single Char
  test("Pickle a case class with a single char field") {
    forAll {
      (obj: SingleChar) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id.toInt, "/avro/single/SingleChar.avsc") === pckl.value)
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
      (obj: SingleChar) =>
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
