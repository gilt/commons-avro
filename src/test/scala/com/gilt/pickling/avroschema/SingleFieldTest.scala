package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs.SingleInt
import scala.pickling._
import com.gilt.pickling.TestUtils._

class SingleFieldTest extends FunSuite with Assertions {

  ignore("Pickle a case class with a single int field") {
    assert(fingerPrint("/avro/single/SingleInt.avsc") === fingerPrint(SingleInt(123).pickle.value))
  }

}
