package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs.SingleInt
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.apache.avro.SchemaBuilder

class SingleFieldTest extends FunSuite with Assertions {

  test("blah") {
    val schema = SchemaBuilder.record("myrecord").namespace("org.example").aliases("oldrecord").fields()
      .requiredString("f0")
      .requiredLong("f1")
      .optionalBoolean("f2").endRecord()

    println(schema)
  }

  ignore("Pickle a case class with a single int field") {
    assert(fingerPrint("/avro/single/SingleInt.avsc") === fingerPrint(SingleInt(123).pickle.value))
  }

}
