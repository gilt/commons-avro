package com.gilt.pickling.avroschema

import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.scalatest.{Assertions, FunSuite}
import java.util.UUID

class ObjectTest extends FunSuite with Assertions{

  test("Generate schema from a case class with a inner case class") {
    assert(fingerPrint("/avro/object/SingleObject.avsc") === fingerPrint(SingleObject(new InnerObject(2)).pickle.value))
    assert(fingerPrint("/avro/object/SingleObject.avsc") === fingerPrint(Schema(SingleObject(new InnerObject(2)))))
    assert(fingerPrint("/avro/object/SingleObject.avsc") === fingerPrint(Schema(classOf[SingleObject])))
    assert(fingerPrint("/avro/object/SingleObject.avsc") === fingerPrint(Schema[SingleObject]))
  }

  test("Generate schema from a case class with a inner some case class") {
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(SingleOptionObject(Some(new InnerObject(2))).pickle.value))
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(Schema(SingleOptionObject(Some(new InnerObject(2))))))
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(Schema(classOf[SingleOptionObject])))
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(Schema[SingleOptionObject]))
  }

  test("Generate schema from a case class with a inner none case class") {
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(SingleOptionObject(Some(new InnerObject(2))).pickle.value))
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(Schema(SingleOptionObject(Some(new InnerObject(2))))))
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(Schema(classOf[SingleOptionObject])))
    assert(fingerPrint("/avro/object/SingleOptionObject.avsc") === fingerPrint(Schema[SingleOptionObject]))
  }

  test("Generate schema from a case class with a list of inner case class") {
    assert(fingerPrint("/avro/object/ListOfObjects.avsc") === fingerPrint(ListOfObjects(List(new InnerObject(2))).pickle.value))
    assert(fingerPrint("/avro/object/ListOfObjects.avsc") === fingerPrint(Schema(ListOfObjects(List(new InnerObject(2))))))
    assert(fingerPrint("/avro/object/ListOfObjects.avsc") === fingerPrint(Schema(classOf[ListOfObjects])))
    assert(fingerPrint("/avro/object/ListOfObjects.avsc") === fingerPrint(Schema[ListOfObjects]))
  }

  test("Generate schema from a case class with a set of inner case class") {
    assert(fingerPrint("/avro/object/SetOfObjects.avsc") === fingerPrint(SetOfObjects(Set(new InnerObject(2))).pickle.value))
    assert(fingerPrint("/avro/object/SetOfObjects.avsc") === fingerPrint(Schema(SetOfObjects(Set(new InnerObject(2))))))
    assert(fingerPrint("/avro/object/SetOfObjects.avsc") === fingerPrint(Schema(classOf[SetOfObjects])))
    assert(fingerPrint("/avro/object/SetOfObjects.avsc") === fingerPrint(Schema[SetOfObjects]))
  }

  test("Generate schema from a case class with a array of inner case class") {
    assert(fingerPrint("/avro/object/ArrayOfObjects.avsc") === fingerPrint(ArrayOfObjects(Array(new InnerObject(2))).pickle.value))
    assert(fingerPrint("/avro/object/ArrayOfObjects.avsc") === fingerPrint(Schema(ArrayOfObjects(Array(new InnerObject(2))))))
    assert(fingerPrint("/avro/object/ArrayOfObjects.avsc") === fingerPrint(Schema(classOf[ArrayOfObjects])))
    assert(fingerPrint("/avro/object/ArrayOfObjects.avsc") === fingerPrint(Schema[ArrayOfObjects]))
  }

  test("Generate schema from a case class with multiple fields of the same inner case class") {
    assert(fingerPrint("/avro/object/MultipleSameObject.avsc") === fingerPrint(MultipleSameObject(new InnerObject(2), new InnerObject(2)).pickle.value))
    assert(fingerPrint("/avro/object/MultipleSameObject.avsc") === fingerPrint(Schema(MultipleSameObject(new InnerObject(2), new InnerObject(2)))))
    assert(fingerPrint("/avro/object/MultipleSameObject.avsc") === fingerPrint(Schema(classOf[MultipleSameObject])))
    assert(fingerPrint("/avro/object/MultipleSameObject.avsc") === fingerPrint(Schema[MultipleSameObject]))
  }

  test("Generate schema from a self referencing case") {
    assert(fingerPrint("/avro/object/SelfReferencingObject.avsc") === fingerPrint(SelfReferencingObject(1, Some(SelfReferencingObject(2, None))).pickle.value))
    assert(fingerPrint("/avro/object/SelfReferencingObject.avsc") === fingerPrint(Schema(SelfReferencingObject(1, Some(SelfReferencingObject(2, None))))))
    assert(fingerPrint("/avro/object/SelfReferencingObject.avsc") === fingerPrint(Schema(classOf[SelfReferencingObject])))
    assert(fingerPrint("/avro/object/SelfReferencingObject.avsc") === fingerPrint(Schema[SelfReferencingObject]))
  }

  test("Generate schema from a case class with a map of inner case class") {
    assert(fingerPrint("/avro/object/MapOfObjects.avsc") === fingerPrint(MapOfObjects(Map("a" -> new InnerObject(2))).pickle.value))
    assert(fingerPrint("/avro/object/MapOfObjects.avsc") === fingerPrint(Schema(MapOfObjects(Map("a" -> new InnerObject(2))))))
    assert(fingerPrint("/avro/object/MapOfObjects.avsc") === fingerPrint(Schema(classOf[MapOfObjects])))
    assert(fingerPrint("/avro/object/MapOfObjects.avsc") === fingerPrint(Schema[MapOfObjects]))
  }

  test("Generate schema from a case class with a UUID") {
    assert(fingerPrint("/avro/object/SingleUUID.avsc") === fingerPrint(SingleUuid(UUID.randomUUID()).pickle.value))
    assert(fingerPrint("/avro/object/SingleUUID.avsc") === fingerPrint(Schema(SingleUuid(UUID.randomUUID()))))
    assert(fingerPrint("/avro/object/SingleUUID.avsc") === fingerPrint(Schema(classOf[SingleUuid])))
    assert(fingerPrint("/avro/object/SingleUUID.avsc") === fingerPrint(Schema[SingleUuid]))
  }

  test("Generate schema from a case class with a multiple UUID") {
    assert(fingerPrint("/avro/object/MultipleUuid.avsc") === fingerPrint(MultipleUuid(UUID.randomUUID(), UUID.randomUUID()).pickle.value))
    assert(fingerPrint("/avro/object/MultipleUuid.avsc") === fingerPrint(Schema(MultipleUuid(UUID.randomUUID(), UUID.randomUUID()))))
    assert(fingerPrint("/avro/object/MultipleUuid.avsc") === fingerPrint(Schema(classOf[MultipleUuid])))
    assert(fingerPrint("/avro/object/MultipleUuid.avsc") === fingerPrint(Schema[MultipleUuid]))
  }

  test("Generate schema from a case class with an option UUID") {
    assert(fingerPrint("/avro/object/SingleOptionUuid.avsc") === fingerPrint(SingleOptionUuid(Some(UUID.randomUUID())).pickle.value))
    assert(fingerPrint("/avro/object/SingleOptionUuid.avsc") === fingerPrint(Schema(SingleOptionUuid(Some(UUID.randomUUID())))))
    assert(fingerPrint("/avro/object/SingleOptionUuid.avsc") === fingerPrint(Schema(classOf[SingleOptionUuid])))
    assert(fingerPrint("/avro/object/SingleOptionUuid.avsc") === fingerPrint(Schema[SingleOptionUuid]))
  }

  test("Generate schema from a case class with a BigDecimal") {
    assert(fingerPrint("/avro/object/SingleBigDecimal.avsc") === fingerPrint(SingleBigDecimal(BigDecimal(1)).pickle.value))
    assert(fingerPrint("/avro/object/SingleBigDecimal.avsc") === fingerPrint(Schema(SingleBigDecimal(BigDecimal(1)))))
    assert(fingerPrint("/avro/object/SingleBigDecimal.avsc") === fingerPrint(Schema(classOf[SingleBigDecimal])))
    assert(fingerPrint("/avro/object/SingleBigDecimal.avsc") === fingerPrint(Schema[SingleBigDecimal]))
  }

  test("Generate schema from a case class with multiple BigDecimal") {
    assert(fingerPrint("/avro/object/MultipleBigDecimal.avsc") === fingerPrint(MultipleBigDecimal(BigDecimal(1), BigDecimal(1)).pickle.value))
    assert(fingerPrint("/avro/object/MultipleBigDecimal.avsc") === fingerPrint(Schema(MultipleBigDecimal(BigDecimal(1), BigDecimal(1)))))
    assert(fingerPrint("/avro/object/MultipleBigDecimal.avsc") === fingerPrint(Schema(classOf[MultipleBigDecimal])))
    assert(fingerPrint("/avro/object/MultipleBigDecimal.avsc") === fingerPrint(Schema[MultipleBigDecimal]))
  }

}
