package com.gilt.pickling.avro

import org.apache.avro.Schema
import com.gilt.pickling.TestObjs.SingleBigDecimal
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.apache.avro.generic.GenericData
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbBigDecimal
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import java.nio.ByteBuffer


object SingleBigDecimalTest {
  implicit val arbSingleBigDecimal = Arbitrary(for (big <- arbBigDecimal.arbitrary) yield SingleBigDecimal(big))
}

class SingleBigDecimalTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {
  import SingleBigDecimalTest._
  test("Pickle a case class with a BigDecimal") {
    forAll {
      (obj: SingleBigDecimal) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj) === pckl.value)
    }
  }

  test("Unpickle a case class with a BigDecimal") {
    forAll {
      (obj: SingleBigDecimal) =>
        val bytes = generateBytesFromAvro(obj)
        val hydratedObj = bytes.unpickle[SingleBigDecimal]
        assert(hydratedObj === obj)
    }
  }

  test("Round trip a case class with a BigDecimal") {
    forAll {
      (obj: SingleBigDecimal) =>
        val pckl = obj.pickle
        val hydratedObj = pckl.unpickle[SingleBigDecimal]
        assert(hydratedObj === obj)
    }
  }

  private def generateBytesFromAvro(obj: SingleBigDecimal) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/SingleBigDecimal.avsc")
    val bigSchema = schema.getField("big").schema()

    val bigRecord = new GenericData.Record(bigSchema)
    bigRecord.put("bigInt", ByteBuffer.wrap(obj.big.underlying().unscaledValue().toByteArray))
    bigRecord.put("scale", obj.big.scale)

    val record = new GenericData.Record(schema)
    record.put("big", bigRecord)

    convertToBytes(schema, record)
  }
}
