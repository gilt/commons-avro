package com.gilt.pickling.avro

import com.gilt.pickling.TestObjs.{InnerObject, SingleOptionObject}
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import org.scalatest.{Assertions, FunSuite}

object SingleObjectOptionInnerTest {
  val someObj = SingleOptionObject(Some(InnerObject(1)))
  val noneObj = SingleOptionObject(None)
}

class SingleObjectOptionInnerTest extends FunSuite with Assertions {

  import SingleObjectOptionInnerTest.someObj

  test("Pickle a case class with some inner case class") {
    val pckl = someObj.pickle
    assert(generateBytesFromAvro(someObj) === pckl.value)
  }

  test("Unpickle a case class with some inner case class") {
    val bytes = generateBytesFromAvro(someObj)
    val hydratedObj: SingleOptionObject = bytes.unpickle[SingleOptionObject]
    assert(someObj === hydratedObj)
  }

  test("Round trip a case class with some inner case class") {
    val pckl = someObj.pickle
    val hydratedObj: SingleOptionObject = pckl.unpickle[SingleOptionObject]
    assert(hydratedObj === someObj)
  }

  private def generateBytesFromAvro(obj: SingleOptionObject) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/SingleOptionObject.avsc")
    val innerSchema = schema.getField("inner").schema().getTypes.get(1)

    val innerRecord = new GenericData.Record(innerSchema)

    val record = new GenericData.Record(schema)
    record.put("inner",
      obj.inner match {
        case Some(x) =>
          innerRecord.put("id", x.id)
          innerRecord
        case _ => null
      }
    )

    convertToBytes(schema, record)
  }
}
