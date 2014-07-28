package com.gilt.pickling.avro

import org.apache.avro.Schema
import com.gilt.pickling.TestObjs.SingleJodaTime
import org.scalatest.{Assertions, FunSuite}
import scala.pickling._
import com.gilt.pickling.TestUtils._
import org.apache.avro.generic.GenericData
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbBigDecimal
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.joda.time.{DateTime, DateTimeZone}
import sun.util.calendar.BaseCalendar.Date


object SingleJodaDateTimeTest {

  implicit val arbitraryDate: Arbitrary[SingleJodaTime] = Arbitrary {
    for {
      ts <- Arbitrary.arbitrary[Long]
      tz <- Arbitrary.arbitrary[Int] if tz >= -23 && tz <= 23
    } yield SingleJodaTime(new DateTime(ts, DateTimeZone.forOffsetHours(tz)))
  }
}

class SingleJodaDateTimeTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {
  import SingleJodaDateTimeTest._
  test("Pickle a case class with a Joda DataTime") {
    forAll {
      (obj: SingleJodaTime) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj) === pckl.value)
    }
  }

  test("Unpickle a case class with a Joda DataTime") {
    forAll {
      (obj: SingleJodaTime) =>
        val bytes = generateBytesFromAvro(obj)
        val hydratedObj = bytes.unpickle[SingleJodaTime]
        assert(hydratedObj === obj)
    }
  }

  test("Round trip a case class with a Joda DataTime") {
    forAll {
      (obj: SingleJodaTime) =>
        val pckl = obj.pickle
        val hydratedObj = pckl.unpickle[SingleJodaTime]
        assert(hydratedObj === obj)
    }
  }

  private def generateBytesFromAvro(obj: SingleJodaTime) = {
    val schema: Schema = retrieveAvroSchemaFromFile("/avro/object/SingleJodaTime.avsc")
    val bigSchema = schema.getField("time").schema()

    val dataTime = new GenericData.Record(bigSchema)
    dataTime.put("timestamp", obj.time.getMillis)
    dataTime.put("timezone", obj.time.getZone.getID)

    val record = new GenericData.Record(schema)
    record.put("time", dataTime)

    convertToBytes(schema, record)
  }
}
