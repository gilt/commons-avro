package com.gilt.pickling.avroschema

import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.scalatest.{Assertions, FunSuite}

class FingerprintObjectTest extends FunSuite with Assertions{

  test("Fingerprint for a case class with a inner case class") {
    assert(Fingerprint(SingleObject(new InnerObject(2))) === fingerPrint(SingleObject(new InnerObject(2)).pickle.value))
  }

  test("Fingerprint for a case class with a inner some case class") {
    assert(Fingerprint(SingleOptionObject(Some(new InnerObject(2)))) === fingerPrint(SingleOptionObject(Some(new InnerObject(2))).pickle.value))
  }

  test("Fingerprint for a case class with a inner none case class") {
    assert(Fingerprint(SingleOptionObject(Some(new InnerObject(2)))) === fingerPrint(SingleOptionObject(Some(new InnerObject(2))).pickle.value))
  }

  test("Fingerprint for a case class with a list of inner case class") {
    assert(Fingerprint(ListOfObjects(List(new InnerObject(2)))) === fingerPrint(ListOfObjects(List(new InnerObject(2))).pickle.value))
  }

  test("Fingerprint for a case class with a set of inner case class") {
    assert(Fingerprint(SetOfObjects(Set(new InnerObject(2)))) === fingerPrint(SetOfObjects(Set(new InnerObject(2))).pickle.value))
  }

  test("Fingerprint for a case class with a array of inner case class") {
    assert(Fingerprint(ArrayOfObjects(Array(new InnerObject(2)))) === fingerPrint(ArrayOfObjects(Array(new InnerObject(2))).pickle.value))
  }

  test("Fingerprint for a case class with multiple fields of the same inner case class") {
    assert(Fingerprint(MultipleSameObject(new InnerObject(2), new InnerObject(2))) === fingerPrint(MultipleSameObject(new InnerObject(2), new InnerObject(2)).pickle.value))
  }

  test("Fingerprint for a self referencing case") {
    assert(Fingerprint(SelfReferencingObject(1, Some(SelfReferencingObject(2, None)))) === fingerPrint(SelfReferencingObject(1, Some(SelfReferencingObject(2, None))).pickle.value))
  }

  test("Fingerprint for a case class with a map of inner case class") {
    assert(Fingerprint(MapOfObjects(Map("a" -> new InnerObject(2)))) === fingerPrint(MapOfObjects(Map("a" -> new InnerObject(2))).pickle.value))
  }

}
