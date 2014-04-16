package com.gilt.pickling

import scala.language.implicitConversions
import scala.pickling._
import java.util.UUID
import java.nio.ByteBuffer

package object avro {
  implicit val pickleFormat = new AvroPickleFormat

  implicit def toAvroPickle(value: Array[Byte]): AvroPickle = AvroPickle(value)

  implicit object UuidPicklerUnpickler extends SPickler[UUID] with Unpickler[UUID] {
    val format = null // not used
    def pickle(picklee: UUID, builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      builder.putField("bytes", b => {
        b.hintTag(implicitly[FastTypeTag[Array[Byte]]])
        b.hintStaticallyElidedType()
        builder.beginEntry(uuidToBytes(picklee))
        builder.endEntry()
      })

      builder.endEntry()
    }

    def unpickle(tag: => FastTypeTag[_], reader: PReader): Any = {
      val reader1 = reader.readField("bytes")
      reader1.hintTag(implicitly[FastTypeTag[Array[Byte]]])
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
}



