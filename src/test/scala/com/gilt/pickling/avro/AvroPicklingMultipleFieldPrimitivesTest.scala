package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.avro.AvroPicklingMultipleFieldPrimitivesTest.MultipleField
import org.apache.avro.generic.GenericData
import scala.pickling._
import avro._
import TestUtils._



object AvroPicklingMultipleFieldPrimitivesTest {

  case class MultipleField(intField: Int,
                           longField: Long,
                           doubleField: Double,
                           floatField: Float,
                           booleanField: Boolean,
                           stringField: String,
                           byteField: Byte,
                           shortField: Short,
                           charField: Char)
}

class AvroPicklingMultipleFieldPrimitivesTest extends FunSuite with Assertions {
  test("Pickle a case class with multiple fields") {
    val obj = MultipleField(1, 2L, 3.0, 4.0F, true, "A string value", 5.toByte, 6.toShort, 'a')
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj, "/avro/MutipleField.avsv") === pckl.value)
  }

  test("Unpickle a case class with multiple fields") {
    val obj = MultipleField(1, 2L, 3.0, 4.0F, true, "A string value", 5.toByte, 6.toShort, 'a')
    val bytes =generateBytesFromAvro(obj, "/avro/MutipleField.avsv")
    val hydratedObj: MultipleField = bytes.unpickle[MultipleField]
    assert(obj === hydratedObj)
  }

  test("Round trip a case class with a single int field") {
    val obj = MultipleField(1, 2L, 3.0, 4.0F, true, "A string value", 5.toByte, 6.toShort, 'a')
    val pckl = obj.pickle
    val hydratedObj: MultipleField = pckl.unpickle[MultipleField]
    assert(hydratedObj === obj)
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
