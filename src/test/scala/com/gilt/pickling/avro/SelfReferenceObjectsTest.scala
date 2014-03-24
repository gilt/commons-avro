package com.gilt.pickling.avro

import com.gilt.pickling.TestObjs.{SelfReferencingObject, SingleOptionObject}
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils._
import scala.Some
import org.apache.avro.generic.GenericData
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._

object ComplexObjectsTest{
  val selfRef = SelfReferencingObject(1, Some(SelfReferencingObject(2, None)))
}

class ComplexObjectsTest extends FunSuite with Assertions{
  import ComplexObjectsTest._

  test("Pickle a case class with self references obj") {
    val pckl = selfRef.pickle
    assert(generateBytesFromAvro(selfRef) === pckl.value)
  }

  test("Unpickle a case class with self references obj") {
    val bytes = generateBytesFromAvro(selfRef)
    val hydratedObj: SelfReferencingObject = bytes.unpickle[SelfReferencingObject]
    assert(selfRef === hydratedObj)
  }

  test("Round trip a case class with self references obj") {
    val pckl = selfRef.pickle
    val hydratedObj: SelfReferencingObject = pckl.unpickle[SelfReferencingObject]
    assert(hydratedObj === selfRef)
  }

  private def generateBytesFromAvro(obj: SelfReferencingObject) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/SelfReferencingObject.avsc")

    val innerRecord = new GenericData.Record(schema)
    obj.inner match {
      case Some(x) =>
        innerRecord.put("id", x.id)
        innerRecord.put("inner", null)
      case _ => null
    }

    val record = new GenericData.Record(schema)
    record.put("id", obj.id)
    record.put("inner", innerRecord)

    convertToBytes(schema, record)
  }
}
