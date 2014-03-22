package com.gilt.pickling.avroschema

import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.scalatest.{Assertions, FunSuite}

class ObjectTest extends FunSuite with Assertions{

  test("Generate schema from a case class with a inner case class") {
    val value = SingleObject(new InnerObject(2)).pickle.value
    println(new String(value))
    assert(fingerPrint("/avro/object/SingleObject.avsc") === fingerPrint(value))
  }

  test("Generate schema from a case class with a inner some case class") {
    val value = SingleOptionObject(Some(new InnerObject(2))).pickle.value
    println(new String(value))
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(value))
  }

  test("Generate schema from a case class with a inner none case class") {
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(SingleOptionObject(Some(new InnerObject(2))).pickle.value))
  }

}
