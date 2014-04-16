package com.gilt.pickling.avro

import org.apache.avro.Schema
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import java.nio.ByteBuffer
import com.gilt.pickling.TestUtils._

import java.util.UUID
import org.apache.avro.generic.GenericData
import com.gilt.pickling.TestObjs.SingleOptionUuid
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import Arbitrary.arbitrary

object SingleOptionUuidTest {
  implicit val genUUID = Arbitrary(Gen.uuid)
  implicit val arbSingleOptionUUID = Arbitrary(for (uuid <- arbitrary[Option[UUID]]) yield SingleOptionUuid(uuid))
}

class SingleOptionUuidTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  import SingleOptionUuidTest._

  test("Pickle a case class with an optional uuid") {
    forAll {
      (obj: SingleOptionUuid) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj) === pckl.value)
    }
  }

  test("Unpickle a case class with an optional uuid") {
    forAll {
      (obj: SingleOptionUuid) =>
        val bytes = generateBytesFromAvro(obj)
        val hydratedObj = bytes.unpickle[SingleOptionUuid]
        assert(obj === hydratedObj)
    }
  }

  test("Round trip a case class with an optional uuid") {
    forAll {
      (obj: SingleOptionUuid) =>
        val pckl = obj.pickle
        val hydratedObj = pckl.unpickle[SingleOptionUuid]
        assert(hydratedObj === obj)
    }
  }

  private def generateBytesFromAvro(obj: SingleOptionUuid) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/SingleOptionUuid.avsc")
    val uuidSchema = schema.getField("uuid").schema().getTypes.get(1)

    val record = new GenericData.Record(schema)

    obj.uuid match {
      case Some(x) =>
        val bytes = ByteBuffer.wrap(new Array[Byte](16))
          .putLong(x.getMostSignificantBits)
          .putLong(x.getLeastSignificantBits)
          .array()

        val fixed = new GenericData.Fixed(uuidSchema, bytes)
        record.put("uuid", fixed)
        fixed
      case _ => null
    }

    convertToBytes(schema, record)
  }
}
