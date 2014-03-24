package com.gilt.pickling.avro

import com.gilt.pickling.TestObjs.{MultipleSameObject, InnerObject}
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils._
import scala.Some
import org.apache.avro.generic.GenericData
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._


object MultipleSameObjectsTest{
  val obj = MultipleSameObject(InnerObject(1), InnerObject(2))
}

class MultipleSameObjectsTest extends FunSuite with Assertions{
   import MultipleSameObjectsTest._

   test("Pickle a case class with multiple same objects") {
     val pckl = obj.pickle
     assert(generateBytesFromAvro(obj) === pckl.value)
   }

   test("Unpickle a case class with multiple same objects") {
     val bytes = generateBytesFromAvro(obj)
     val hydratedObj: MultipleSameObject = bytes.unpickle[MultipleSameObject]
     assert(obj === hydratedObj)
   }

   test("Round trip a case class with multiple same objects") {
     val pckl = obj.pickle
     val hydratedObj: MultipleSameObject = pckl.unpickle[MultipleSameObject]
     assert(hydratedObj === obj)
   }

   private def generateBytesFromAvro(obj: MultipleSameObject) = {
     val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/MultipleSameObject.avsc")

     val innerSchema = schema.getField("first").schema()

     val innerRecordA = new GenericData.Record(innerSchema)
     innerRecordA.put("id", obj.first.id)

     val innerRecordB = new GenericData.Record(innerSchema)
     innerRecordB.put("id", obj.second.id)

     val record = new GenericData.Record(schema)
     record.put("first", innerRecordA)
     record.put("second", innerRecordB)

     convertToBytes(schema, record)
   }
 }
