package com.gilt.pickling

import scala.language.implicitConversions
import scala.pickling._
import java.util.UUID
import java.nio.ByteBuffer
import java.math.BigInteger
import org.joda.time.{DateTimeZone, DateTime}

package object avro {
  implicit val pickleFormat = new AvroPickleFormat

  implicit def toAvroPickle(value: Array[Byte]): AvroPickle = AvroPickle(value)

  implicit object UuidPicklerUnpickler extends SPickler[UUID] with Unpickler[UUID] {
    val format = null // not used
    def pickle(picklee: UUID, builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      builder.putField("bytes", b => {
        b.hintTag(FastTypeTag.ArrayByte)
        b.hintStaticallyElidedType()
        builder.beginEntry(uuidToBytes(picklee))
        builder.endEntry()
      })

      builder.endEntry()
    }

    def unpickle(tag: => FastTypeTag[_], reader: PReader): Any = {
      val reader1 = reader.readField("bytes")
      reader1.hintTag(FastTypeTag.ArrayByte)
      reader1.hintStaticallyElidedType()

      reader1.beginEntry()
      val bytes = reader1.readPrimitive().asInstanceOf[Array[Byte]]
      reader1.endEntry()

      bytesToUUID(bytes)
    }

    private def uuidToBytes(uuid: UUID): Array[Byte] =
      ByteBuffer.wrap(new Array[Byte](16))
        .putLong(uuid.getMostSignificantBits)
        .putLong(uuid.getLeastSignificantBits)
        .array()

    private def bytesToUUID(bytes: Array[Byte]): UUID = {
      val buffer = ByteBuffer.wrap(bytes)
      new UUID(buffer.getLong, buffer.getLong)
    }
  }

  implicit object BigDecimalPicklerUnpickler extends SPickler[java.math.BigDecimal] with Unpickler[java.math.BigDecimal] {
    val format = null // not used
    def pickle(picklee: java.math.BigDecimal, builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      val unscaled = picklee.unscaledValue()

      builder.putField("bitInt", b => {
        b.hintTag(FastTypeTag.ArrayByte)
        b.hintStaticallyElidedType()
        builder.beginEntry(unscaled.toByteArray)
        builder.endEntry()
      })

      builder.putField("scale", b => {
        b.hintTag(FastTypeTag.Int)
        b.hintStaticallyElidedType()
        builder.beginEntry(picklee.scale)
        builder.endEntry()
      })

      builder.endEntry()
    }

    def unpickle(tag: => FastTypeTag[_], reader: PReader): Any = {

      val reader1 = reader.readField("bitInt")
      reader1.hintTag(FastTypeTag.ArrayByte)
      reader1.hintStaticallyElidedType()
      reader1.beginEntry()
      val bigIntValue = reader1.readPrimitive().asInstanceOf[Array[Byte]]
      reader1.endEntry()

      val reader2 = reader.readField("scale")
      reader2.hintTag(FastTypeTag.Int)
      reader2.hintStaticallyElidedType()
      reader2.beginEntry()
      val scale = reader2.readPrimitive().asInstanceOf[Int]
      reader2.endEntry()

      new java.math.BigDecimal(new BigInteger(bigIntValue), scale)
    }
  }


  implicit object SBigDecimalPicklerUnpickler extends SPickler[BigDecimal] with Unpickler[BigDecimal] {
    val format = null // not used
    def pickle(picklee: BigDecimal, builder: PBuilder): Unit =
      BigDecimalPicklerUnpickler.pickle(picklee.underlying(), builder)

    def unpickle(tag: => FastTypeTag[_], reader: PReader): Any =
      BigDecimal(BigDecimalPicklerUnpickler.unpickle(tag, reader).asInstanceOf[java.math.BigDecimal])
  }

  implicit object DateTimePicklerUnpickler extends SPickler[DateTime] with Unpickler[DateTime] {
    val format = null // not used
    def pickle(picklee: DateTime, builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      val iosDataTime = picklee.toDateTimeISO
      val timestamp = iosDataTime.getMillis
      val timezone = iosDataTime.getZone.getID

      builder.putField("timestamp", b => {
        b.hintTag(FastTypeTag.Long)
        b.hintStaticallyElidedType()
        builder.beginEntry(timestamp)
        builder.endEntry()
      })

      builder.putField("timezone", b => {
        b.hintTag(FastTypeTag.ScalaString)
        b.hintStaticallyElidedType()
        builder.beginEntry(timezone)
        builder.endEntry()
      })

      builder.endEntry()
    }

    def unpickle(tag: => FastTypeTag[_], reader: PReader): Any = {

      val reader1 = reader.readField("timestamp")
      reader1.hintTag(FastTypeTag.Long)
      reader1.hintStaticallyElidedType()
      reader1.beginEntry()
      val timestamp = reader1.readPrimitive().asInstanceOf[Long]
      reader1.endEntry()

      val reader2 = reader.readField("timezone")
      reader2.hintTag(FastTypeTag.ScalaString)
      reader2.hintStaticallyElidedType()
      reader2.beginEntry()
      val timezone = reader2.readPrimitive().asInstanceOf[String]
      reader2.endEntry()

      new DateTime(timestamp, DateTimeZone.forID(timezone))
    }
  }
}



