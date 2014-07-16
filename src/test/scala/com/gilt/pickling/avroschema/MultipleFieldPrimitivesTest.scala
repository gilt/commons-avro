package com.gilt.pickling.avroschema

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._


class MultipleFieldPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  test("Generate schema from a case class with multiple fields") {
    assert(fingerPrint("/avro/MutipleField.avsc") === fingerPrint(MultipleField(1, 2L, 3D, 4F, true, "ABC", 5.toByte, 6.toShort, 'c').pickle.value))
    assert(fingerPrint("/avro/MutipleField.avsc") === fingerPrint(Schema(MultipleField(1, 2L, 3D, 4F, true, "ABC", 5.toByte, 6.toShort, 'c'))))
    assert(fingerPrint("/avro/MutipleField.avsc") === fingerPrint(Schema(classOf[MultipleField])))
    assert(fingerPrint("/avro/MutipleField.avsc") === fingerPrint(Schema[MultipleField]))
  }
}
