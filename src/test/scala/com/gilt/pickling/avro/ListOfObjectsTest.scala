package com.gilt.pickling.avro

import org.apache.avro.Schema
import com.gilt.pickling.TestUtils._
import com.gilt.pickling.TestObjs.{InnerObject, ListOfObjects}
import org.apache.avro.generic.GenericData
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import scala.collection.JavaConverters._


object ListOfObjectsTest {
  val obj = ListOfObjects(List(InnerObject(1), InnerObject(2), InnerObject(3)))
}

class ListOfObjectsTest extends FunSuite with Assertions {

  import ListOfObjectsTest.obj

  test("Pickle a case class with a list of objects") {
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj) === pckl.value)
  }

  test("Unpickle a case class with a list of objects") {
    val bytes = generateBytesFromAvro(obj)
    val hydratedObj: ListOfObjects = bytes.unpickle[ListOfObjects]
    assert(obj === hydratedObj)
  }

  test("Round trip a case class with a list of objects") {
    val pckl = obj.pickle
    val hydratedObj: ListOfObjects = pckl.unpickle[ListOfObjects]
    assert(hydratedObj === obj)
  }

  private def generateBytesFromAvro(obj: ListOfObjects) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/ListOfObjects.avsc")
    val innerSchema = schema.getField("list").schema().getElementType

    val list = obj.list.map{ inner =>
      val innerRecord = new GenericData.Record(innerSchema)
      innerRecord.put("id", inner.id)
      innerRecord
    }

    val record = new GenericData.Record(schema)
    record.put("list", list.asJava)

    convertToBytes(schema, record)
  }
}
