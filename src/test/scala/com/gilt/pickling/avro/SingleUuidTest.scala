package com.gilt.pickling.avro

import org.apache.avro.Schema
import com.gilt.pickling.TestObjs.SingleUuid
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

object SingleUuidTest {
  implicit val arbSingleUUID = Arbitrary(for (uuid <- Gen.uuid) yield SingleUuid(uuid))
}

class SingleUuidTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  import SingleUuidTest._

  test("Pickle a case class with a uuid") {
    forAll {
      (obj: SingleUuid) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj) === pckl.value)
    }
  }

  test("Unpickle a case class with a uuid") {
    forAll {
      (obj: SingleUuid) =>
        val bytes = generateBytesFromAvro(obj)
        val hydratedObj = bytes.unpickle[SingleUuid]
        assert(obj === hydratedObj)
    }
  }

  test("Round trip a case class with a uuid") {
    forAll {
      (obj: SingleUuid) =>
        val pckl = obj.pickle
        val hydratedObj = pckl.unpickle[SingleUuid]
        assert(hydratedObj === obj)
    }
  }

  private def generateBytesFromAvro(obj: SingleUuid) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/SingleUuid.avsc")
    val uuidSchema = schema.getField("uuid").schema()

    val record = new GenericData.Record(schema)

    val bytes = ByteBuffer.wrap(new Array[Byte](16))
      .putLong(obj.uuid.getMostSignificantBits)
      .putLong(obj.uuid.getLeastSignificantBits)
      .array()

    val fixed = new GenericData.Fixed(uuidSchema, bytes)
    record.put("uuid", fixed)

    convertToBytes(schema, record)
  }
}
