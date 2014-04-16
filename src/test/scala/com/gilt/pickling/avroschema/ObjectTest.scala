package com.gilt.pickling.avroschema

import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.scalatest.{Assertions, FunSuite}
import java.util.UUID

class ObjectTest extends FunSuite with Assertions{

  test("Generate schema from a case class with a inner case class") {
    assert(fingerPrint("/avro/object/SingleObject.avsc") === fingerPrint(SingleObject(new InnerObject(2)).pickle.value))
  }

  test("Generate schema from a case class with a inner some case class") {
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(SingleOptionObject(Some(new InnerObject(2))).pickle.value))
  }

  test("Generate schema from a case class with a inner none case class") {
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(SingleOptionObject(Some(new InnerObject(2))).pickle.value))
  }

  test("Generate schema from a case class with a list of inner case class") {
    assert(fingerPrint("/avro/object/ListOfObjects.avsc") === fingerPrint(ListOfObjects(List(new InnerObject(2))).pickle.value))
  }

  test("Generate schema from a case class with a set of inner case class") {
    assert(fingerPrint("/avro/object/SetOfObjects.avsc") === fingerPrint(SetOfObjects(Set(new InnerObject(2))).pickle.value))
  }

  test("Generate schema from a case class with a array of inner case class") {
    assert(fingerPrint("/avro/object/ArrayOfObjects.avsc") === fingerPrint(ArrayOfObjects(Array(new InnerObject(2))).pickle.value))
  }

  test("Generate schema from a case class with multiple fields of the same inner case class") {
    assert(fingerPrint("/avro/object/MultipleSameObject.avsc") === fingerPrint(MultipleSameObject(new InnerObject(2), new InnerObject(2)).pickle.value))
  }

  test("Generate schema from a self referencing case") {
    assert(fingerPrint("/avro/object/SelfReferencingObject.avsc") === fingerPrint(SelfReferencingObject(1, Some(SelfReferencingObject(2, None))).pickle.value))
  }

  test("Generate schema from a case class with a map of inner case class") {
    assert(fingerPrint("/avro/object/MapOfObjects.avsc") === fingerPrint(MapOfObjects(Map("a" -> new InnerObject(2))).pickle.value))
  }

  test("Generate schema from a case class with a UUID") {
    assert(fingerPrint("/avro/object/SingleUuid.avsc") === fingerPrint(SingleUuid(UUID.randomUUID()).pickle.value))
  }

  test("Generate schema from a case class with an option UUID") {
    assert(fingerPrint("/avro/object/SingleOptionUuid.avsc") === fingerPrint(SingleOptionUuid(Some(UUID.randomUUID())).pickle.value))
  }
}
