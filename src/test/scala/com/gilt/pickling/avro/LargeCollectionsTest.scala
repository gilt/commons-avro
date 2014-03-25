package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils._
import com.gilt.pickling.TestObjs.{InnerObject, ListOfObjects, ArrayOfInts, ListOfInts}
import org.apache.avro.generic.GenericData
import java.util.{List => JList}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object LargeCollectionsTest {
  val objListInt = new ListOfInts((1 to 10000).toList)
  val objArrayInt = new ArrayOfInts((1 to 10000).toArray)
  val objListOfObjs = new ListOfObjects((1 to 10000).map(x => InnerObject(x)).toList)
}

class LargeCollectionsTest extends FunSuite with Assertions {

  import LargeCollectionsTest._

  // List of Ints
  test("Pickle a case class with an list of ints") {
    val pckl = objListInt.pickle
    val depickled = generateBytesFromAvro(objListInt.list, "/avro/lists/ListOfInts.avsc")
    assert(depickled === pckl.value)
  }

  test("Unpickle a case class with an list of ints") {
    val bytes = generateBytesFromAvro(objListInt.list, "/avro/lists/ListOfInts.avsc")
    val hydratedObj: ListOfInts = bytes.unpickle[ListOfInts]
    assert(objListInt.list === hydratedObj.list)
  }

  test("Round trip a case class with an list of ints") {
    val pckl = objListInt.pickle
    val hydratedObj: ListOfInts = pckl.unpickle[ListOfInts]
    assert(objListInt.list === hydratedObj.list)
  }

  // Array of Ints
  test("Pickle a case class with an array of ints") {
    val pckl = objListInt.pickle
    val depickled = generateBytesFromAvro(objArrayInt.list.toList, "/avro/lists/ListOfInts.avsc")
    assert(depickled === pckl.value)
  }

  test("Unpickle a case class with an array of ints") {
    val bytes = generateBytesFromAvro(objArrayInt.list.toList, "/avro/lists/ListOfInts.avsc")
    val hydratedObj: ArrayOfInts = bytes.unpickle[ArrayOfInts]
    assert(objArrayInt.list === hydratedObj.list)
  }

  test("Round trip a case class with an arry of ints") {
    val pckl = objListInt.pickle
    val hydratedObj: ArrayOfInts = pckl.unpickle[ArrayOfInts]
    assert(objArrayInt.list === hydratedObj.list)
  }

  // List of Objects
  test("Pickle a case class with an list of objects") {
    val pckl = objListOfObjs.pickle
    val depickled = generateBytesFromAvro(objListOfObjs)
    assert(depickled === pckl.value)
  }

  test("Unpickle a case class with an list of objects") {
    val bytes = generateBytesFromAvro(objListOfObjs)
    val hydratedObj: ListOfObjects = bytes.unpickle[ListOfObjects]
    assert(objListOfObjs === hydratedObj)
  }

  test("Round trip a case class with an list of objects") {
    val pckl = objListOfObjs.pickle
    val hydratedObj: ListOfObjects = pckl.unpickle[ListOfObjects]
    assert(objListOfObjs === hydratedObj)
  }

  private def generateBytesFromAvro(value: JList[_], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("list", value) // need java collection at this point
    convertToBytes(schema, record)
  }

  private def generateBytesFromAvro(obj: ListOfObjects) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/ListOfObjects.avsc")
    val innerSchema = schema.getField("list").schema().getElementType

    val list = obj.list.map {
      inner =>
        val innerRecord = new GenericData.Record(innerSchema)
        innerRecord.put("id", inner.id)
        innerRecord
    }

    val record = new GenericData.Record(schema)
    record.put("list", list.asJava)

    convertToBytes(schema, record)
  }
}
