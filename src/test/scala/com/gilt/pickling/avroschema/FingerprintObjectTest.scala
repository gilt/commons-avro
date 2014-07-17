package com.gilt.pickling.avroschema

import com.gilt.pickling.TestObjs._
import com.gilt.pickling.TestUtils._
import org.scalatest.{Assertions, FunSuite}
import java.util.UUID

class FingerprintObjectTest extends FunSuite with Assertions{

  test("Fingerprint for a case class with a inner case class") {
    assert(Fingerprint(SingleObject(new InnerObject(2))) === fingerPrint(Schema[SingleObject]))
    assert(Fingerprint(Schema[SingleObject]) === fingerPrint(Schema[SingleObject]))
  }

  test("Fingerprint for a case class with a inner some case class") {
    assert(Fingerprint(SingleOptionObject(Some(new InnerObject(2)))) === fingerPrint(Schema[SingleOptionObject]))
    assert(Fingerprint(Schema[SingleOptionObject]) === fingerPrint(Schema[SingleOptionObject]))
  }

  test("Fingerprint for a case class with a inner none case class") {
    assert(Fingerprint(SingleOptionObject(Some(new InnerObject(2)))) === fingerPrint(Schema[SingleOptionObject]))
    assert(Fingerprint(Schema[SingleOptionObject]) === fingerPrint(Schema[SingleOptionObject]))
  }

  test("Fingerprint for a case class with a list of inner case class") {
    assert(Fingerprint(ListOfObjects(List(new InnerObject(2)))) === fingerPrint(Schema[ListOfObjects]))
    assert(Fingerprint(Schema[ListOfObjects]) === fingerPrint(Schema[ListOfObjects]))
  }

  test("Fingerprint for a case class with a set of inner case class") {
    assert(Fingerprint(SetOfObjects(Set(new InnerObject(2)))) === fingerPrint(Schema[SetOfObjects]))
    assert(Fingerprint(Schema[SetOfObjects]) === fingerPrint(Schema[SetOfObjects]))
  }

  test("Fingerprint for a case class with a array of inner case class") {
    assert(Fingerprint(ArrayOfObjects(Array(new InnerObject(2)))) === fingerPrint(Schema[ArrayOfObjects]))
    assert(Fingerprint(Schema[ArrayOfObjects]) === fingerPrint(Schema[ArrayOfObjects]))
  }

  test("Fingerprint for a case class with multiple fields of the same inner case class") {
    assert(Fingerprint(MultipleSameObject(new InnerObject(2), new InnerObject(2))) === fingerPrint(Schema[MultipleSameObject]))
    assert(Fingerprint(Schema[MultipleSameObject]) === fingerPrint(Schema[MultipleSameObject]))
  }

  test("Fingerprint for a self referencing case") {
    assert(Fingerprint(SelfReferencingObject(1, Some(SelfReferencingObject(2, None)))) === fingerPrint(Schema[SelfReferencingObject]))
    assert(Fingerprint(Schema[SelfReferencingObject]) === fingerPrint(Schema[SelfReferencingObject]))
  }

  test("Fingerprint for a case class with a map of inner case class") {
    assert(Fingerprint(MapOfObjects(Map("a" -> new InnerObject(2)))) === fingerPrint(Schema[MapOfObjects]))
    assert(Fingerprint(Schema[MapOfObjects]) === fingerPrint(Schema[MapOfObjects]))
  }

  test("Fingerprint from a case class with a UUID") {
    assert(Fingerprint(SingleUuid(UUID.randomUUID())) === fingerPrint(Schema[SingleUuid]))
    assert(Fingerprint(Schema[SingleUuid]) === fingerPrint(Schema[SingleUuid]))
  }

  test("Fingerprint from a case class with a multiple UUID") {
    assert(Fingerprint(MultipleUuid(UUID.randomUUID(), UUID.randomUUID())) === fingerPrint(Schema[MultipleUuid]))
    assert(Fingerprint(Schema[MultipleUuid]) === fingerPrint(Schema[MultipleUuid]))
  }

  test("Fingerprint from a case class with an option UUID") {
    assert(Fingerprint(SingleOptionUuid(Some(UUID.randomUUID()))) === fingerPrint(Schema[SingleOptionUuid]))
    assert(Fingerprint(Schema[SingleOptionUuid]) === fingerPrint(Schema[SingleOptionUuid]))
  }
}
