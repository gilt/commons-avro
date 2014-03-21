package com.gilt.pickling.avro

import org.apache.avro.Schema
import com.gilt.pickling.TestUtils._
import com.gilt.pickling.TestObjs.{InnerObject, ArrayOfObjects}
import org.apache.avro.generic.GenericData
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import scala.collection.JavaConverters._

object ArrayOfObjectsTest {
  val obj = ArrayOfObjects(Array(InnerObject(1), InnerObject(2), InnerObject(3)))
}

class ArrayOfObjectsTest extends FunSuite with Assertions {

   import ArrayOfObjectsTest.obj

   test("Pickle a case class with a array of objects") {
     val pckl = obj.pickle
     assert(generateBytesFromAvro(obj) === pckl.value)
   }

   test("Unpickle a case class with a array of objects") {
     val bytes = generateBytesFromAvro(obj)
     val hydratedObj: ArrayOfObjects = bytes.unpickle[ArrayOfObjects]
     assert(obj.list === hydratedObj.list)
   }

   test("Round trip a case class with a array of objects") {
     val pckl = obj.pickle
     val hydratedObj: ArrayOfObjects = pckl.unpickle[ArrayOfObjects]
     assert(hydratedObj.list === obj.list)
   }

   private def generateBytesFromAvro(obj: ArrayOfObjects) = {
     val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/ArrayOfObjects.avsc")
     val innerSchema = schema.getField("list").schema().getElementType

     val list = obj.list.map{ inner =>
       val innerRecord = new GenericData.Record(innerSchema)
       innerRecord.put("id", inner.id)
       innerRecord
     }

     val record = new GenericData.Record(schema)
     record.put("list", list.toList.asJava)

     convertToBytes(schema, record)
   }
 }
