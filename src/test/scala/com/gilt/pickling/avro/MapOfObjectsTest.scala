package com.gilt.pickling.avro

import org.apache.avro.Schema
import com.gilt.pickling.TestUtils._
import com.gilt.pickling.TestObjs.{InnerObject, MapOfObjects, SetOfObjects}
import org.apache.avro.generic.GenericData
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import scala.collection.JavaConverters._
import org.apache.avro.generic.GenericData.Record

object MapOfObjectsTest {
  val obj = MapOfObjects(Map("a" -> InnerObject(1), "b" -> InnerObject(2)))
}

class MapOfObjectsTest extends FunSuite with Assertions {

  import MapOfObjectsTest.obj

  test("Pickle a case class with a map of objects") {
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj) === pckl.value)
  }

  test("Unpickle a case class with a map of objects") {
    val bytes = generateBytesFromAvro(obj)
    val hydratedObj: MapOfObjects = bytes.unpickle[MapOfObjects]
    assert(obj === hydratedObj)
  }

  test("Round trip a case class with a map of objects") {
    val pckl = obj.pickle
    val hydratedObj: MapOfObjects = pckl.unpickle[MapOfObjects]
    assert(hydratedObj === obj)
  }

  private def generateBytesFromAvro(obj: MapOfObjects) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/MapOfObjects.avsc")
    val innerSchema = schema.getField("list").schema().getValueType


    val list: Map[String, Record] = obj.list.mapValues{ x =>
      val innerRecord = new GenericData.Record(innerSchema)
      innerRecord.put("id", x.id)
      innerRecord
    }

    val record = new GenericData.Record(schema)
    record.put("list", list.asJava)

    convertToBytes(schema, record)
  }
}
