package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import scala.pickling._
import com.gilt.pickling.TestUtils
import TestUtils._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import com.gilt.pickling.TestObjs._

object MultipleFieldPrimitivesTest {

  import org.scalacheck.Gen
  val genField: Gen[MultipleField] = for {
    intField <- Gen.choose(Int.MinValue, Int.MaxValue)
    longField <- Gen.choose(Long.MinValue, Long.MaxValue)
    doubleField <- Gen.chooseNum(Double.MinValue, Double.MaxValue)
    floatField <- Gen.chooseNum(Float.MinValue, Float.MaxValue)
    booleanField <- Gen.oneOf(true, false)
    stringField <- Gen.alphaStr
    byteField <- Gen.choose(Byte.MinValue, Byte.MaxValue)
    shortField <- Gen.choose(Short.MinValue, Short.MaxValue)
    charField <- Gen.choose(Char.MinValue, Char.MaxValue)
  } yield {
    MultipleField(intField, longField, doubleField, floatField,
      booleanField, stringField, byteField, shortField, charField)
  }
}

class MultipleFieldPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  import MultipleFieldPrimitivesTest.genField

  test("Pickle a case class with multiple fields") {
    forAll(genField) {
      obj =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj, "/avro/MutipleField.avsv") === pckl.value)
    }
  }

  test("Unpickle a case class with multiple fields") {
    forAll(genField) {
      obj =>
        val bytes = generateBytesFromAvro(obj, "/avro/MutipleField.avsv")
        val hydratedObj: MultipleField = bytes.unpickle[MultipleField]
        assert(obj === hydratedObj)
    }
  }

  test("Round trip a case class with a single int field") {
    forAll(genField) {
      obj =>
        val pckl = obj.pickle
        val hydratedObj: MultipleField = pckl.unpickle[MultipleField]
        assert(hydratedObj === obj)
    }
  }

  private def generateBytesFromAvro(value: MultipleField, schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("intField", value.intField)
    record.put("longField", value.longField)
    record.put("doubleField", value.doubleField)
    record.put("floatField", value.floatField)
    record.put("booleanField", value.booleanField)
    record.put("stringField", value.stringField)
    record.put("byteField", value.byteField.toInt)
    record.put("shortField", value.shortField.toInt)
    record.put("charField", value.charField.toInt)
    convertToBytes(schema, record)
  }
}
