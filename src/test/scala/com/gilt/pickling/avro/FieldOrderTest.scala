package com.gilt.pickling.avro

import org.apache.avro.Schema
import com.gilt.pickling.TestUtils._
import com.gilt.pickling.TestObjs.{FieldOrdering, FieldOrderingInner}
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import org.apache.avro.io.DecoderFactory
import org.apache.avro.generic.{GenericDatumReader, GenericData}
import org.joda.time.DateTime
import java.util.UUID

object FieldOrderTest{
  val obj = FieldOrdering(FieldOrderingInner(Some(1), 23), Some(1), List(1), Map("1" -> 1), DateTime.now, UUID.randomUUID(), BigDecimal(1), Array(1,2,3), 1, "someString")
}

class FieldOrderTest extends FunSuite with Assertions{
  import FieldOrderTest.obj

  test("Validate that fields are written out is correct order") {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/FieldOrdering.avsc")
    val pckl = obj.pickle
    val hydrateObj = pckl.unpickle[FieldOrdering]

    assert(obj.inner === hydrateObj.inner)
    assert(obj.optionalValue === hydrateObj.optionalValue)
    assert(obj.someMap === hydrateObj.someMap)
    assert(obj.someList === hydrateObj.someList)
    assert(obj.dateTimeField === hydrateObj.dateTimeField)
    assert(obj.bigDecimal === hydrateObj.bigDecimal)
    assert(obj.arrayField.sameElements(hydrateObj.arrayField))
    assert(obj.stringField == hydrateObj.stringField)

    val reader = new GenericDatumReader[GenericData.Record](schema)
    val decoder = DecoderFactory.get.binaryDecoder(pckl.value, null)
    reader.read(null, decoder) // validates the that field are written out correctly
  }
}
