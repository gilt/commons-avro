package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.avro.TestUtils._
import com.gilt.pickling.avro.AvroPickleSingleOptionalFieldPrimitivesTest.SingleOptionInt
import scala.Some
import org.apache.avro.generic.GenericData
import scala.pickling._
import avro._


object AvroPickleSingleOptionalFieldPrimitivesTest {
  case class SingleOptionInt(id: Option[Int])
  case class SingleOptionLong(id: Option[Long])
  case class SingleOptionDouble(id: Option[Double])
  case class SingleOptionFloat(id: Option[Float])
  case class SingleOptionBoolean(id: Option[Boolean])
  case class SingleOptionString(id: Option[String])
  case class SingleOptionByte(id: Option[Byte])
  case class SingleOptionShort(id: Option[Short])
  case class SingleOptionChar(id: Option[Char])
}

class AvroPickleSingleOptionalFieldPrimitivesTest extends FunSuite with Assertions{
  //Single Int
  ignore("Pickle a case class with a single optional int field") {
    val obj = new SingleOptionInt(Some(123))
    val pckl = obj.pickle
    assert(generateSingleValueBytesFromAvro(Some(123), "/avro/option/SingleOptionInt.avsc") === pckl.value)
  }

  ignore("Unpickle a case class with a single optional int field") {
    val bytes = generateSingleValueBytesFromAvro(Some(123), "/avro/option/SingleOptionInt.avsc")
    val obj: SingleOptionInt = bytes.unpickle[SingleOptionInt]
    assert(obj === new SingleOptionInt(Some(123)))
  }

  ignore("Round trip a case class with a single optional int field") {
    val obj = new SingleOptionInt(Some(123))
    val pckl = obj.pickle
    val hydratedObj: SingleOptionInt = pckl.unpickle[SingleOptionInt]
    assert(hydratedObj === obj)
  }

  private def generateSingleValueBytesFromAvro(value: Some[Any], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    value match {
      case Some(x) => record.put("id", x)
      case _ => record.put("id", null)
    }
    convertToBytes(schema, record)
  }
}
