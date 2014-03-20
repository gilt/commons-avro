package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import com.gilt.pickling.TestObjs.{InnerObject, SingleObject}
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class SingleObjectWithInnerObjectTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  test("Pickle a case class with inner case class") {
    val obj = SingleObject(new InnerObject(1))
    val pckl = obj.pickle
    assert(generateSingleObjectValueBytesFromAvro(obj) === pckl.value)
  }

  test("Unpickle a case class with multiple fields") {
    val obj = SingleObject(new InnerObject(1))
    val bytes = generateSingleObjectValueBytesFromAvro(obj)
    val hydratedObj: SingleObject = bytes.unpickle[SingleObject]
    assert(obj === hydratedObj)
  }

  test("Round trip a case class with a single int field") {
    val obj = SingleObject(new InnerObject(1))
    val pckl = obj.pickle
    val hydratedObj: SingleObject = pckl.unpickle[SingleObject]
    assert(hydratedObj === obj)
  }

  private def generateSingleObjectValueBytesFromAvro(obj: SingleObject) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/SingleObject.avsc")
    val innerSchema = schema.getField("inner").schema()

    val innerRecord = new GenericData.Record(innerSchema)
    innerRecord.put("id", 1)

    val record = new GenericData.Record(schema)
    record.put("inner", innerRecord)

    convertToBytes(schema, record)
  }
}
