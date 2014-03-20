package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs.{InnerObject, SingleObject}
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

object SingleObjectInnerTest{
  val obj = SingleObject(new InnerObject(1))
}

class SingleObjectInnerTest extends FunSuite with Assertions {
  import SingleObjectInnerTest.obj
  test("Pickle a case class with inner case class") {
    val pckl = obj.pickle
    assert(generateSingleObjectValueBytesFromAvro(obj) === pckl.value)
  }

  test("Unpickle a case class with inner case class") {
    val bytes = generateSingleObjectValueBytesFromAvro(obj)
    val hydratedObj: SingleObject = bytes.unpickle[SingleObject]
    assert(obj === hydratedObj)
  }

  test("Round trip a case class with inner case class") {
    val pckl = obj.pickle
    val hydratedObj: SingleObject = pckl.unpickle[SingleObject]
    assert(hydratedObj === obj)
  }

  private def generateSingleObjectValueBytesFromAvro(obj: SingleObject) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/SingleObject.avsc")
    val innerSchema = schema.getField("inner").schema()

    val innerRecord = new GenericData.Record(innerSchema)
    innerRecord.put("id", obj.inner.id)

    val record = new GenericData.Record(schema)
    record.put("inner", innerRecord)

    convertToBytes(schema, record)
  }
}
