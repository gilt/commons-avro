package com.gilt.pickling.avro

import scala.pickling.{PicklingException, PBuilder, PickleTools}

final class AvroPickleBuilder(format: AvroPickleFormat, buffer: AvroEncodingOutput = new AvroEncodingOutput()) extends PBuilder with PickleTools {
  import com.gilt.pickling.util.Types._

  @inline def beginEntry(picklee: Any): PBuilder = withHints {
    hints =>
      hints.tag.key match {
        case KEY_INT => buffer.encodeIntTo(picklee.asInstanceOf[Int])
        case KEY_LONG => buffer.encodeLongTo(picklee.asInstanceOf[Long])
        case KEY_FLOAT => buffer.encodeFloatTo(picklee.asInstanceOf[Float])
        case KEY_DOUBLE => buffer.encodeDoubleTo(picklee.asInstanceOf[Double])
        case KEY_BOOLEAN => buffer.encodeBooleanTo(picklee.asInstanceOf[Boolean])
        case KEY_SCALA_STRING | KEY_JAVA_STRING => buffer.encodeStringTo(picklee.asInstanceOf[String])
        case KEY_BYTE => buffer.encodeByteTo(picklee.asInstanceOf[Byte])
        case KEY_SHORT => buffer.encodeShortTo(picklee.asInstanceOf[Short])
        case KEY_CHAR => buffer.encodeCharTo(picklee.asInstanceOf[Char])
        case KEY_UNIT | KEY_NULL => throw new PicklingException("Not supported.")
        case KEY_ARRAY_INT => buffer.encodeIntArrayTo(picklee.asInstanceOf[Array[Int]])
        case KEY_ARRAY_LONG => buffer.encodeLongArrayTo(picklee.asInstanceOf[Array[Long]])
        case KEY_ARRAY_FLOAT => buffer.encodeFloatArrayTo(picklee.asInstanceOf[Array[Float]])
        case KEY_ARRAY_DOUBLE => buffer.encodeDoubleArrayTo(picklee.asInstanceOf[Array[Double]])
        case KEY_ARRAY_BOOLEAN => buffer.encodeBooleanArrayTo(picklee.asInstanceOf[Array[Boolean]])
        case KEY_ARRAY_BYTE => buffer.encodeByteArrayTo(picklee.asInstanceOf[Array[Byte]])
        case KEY_ARRAY_SHORT => buffer.encodeShortArrayTo(picklee.asInstanceOf[Array[Short]])
        case KEY_ARRAY_CHAR => buffer.encodeCharArrayTo(picklee.asInstanceOf[Array[Char]])
        case KEY_NIL => buffer.encodeByteArrayTo(Array.empty)
        case KEY_NONE => buffer.encodeLongTo(0)
        case key if key.startsWith(KEY_SOME) => buffer.encodeLongTo(1)
        case key =>
      }
      this
  }

  @inline def putField(name: String, pickler: PBuilder => Unit): PBuilder = {
    pickler(this)
    this
  }

  @inline def endEntry(): Unit = {}

  @inline def beginCollection(length: Int): PBuilder = {
    //TODO We need to deal with maps. But Sets and Lists can be handle this way
    buffer.encoder.writeArrayStart()
    buffer.encoder.setItemCount(length)
    this
  }

  @inline def putElement(pickler: PBuilder => Unit): PBuilder = {
    buffer.encoder.startItem()
    pickler(this)
    this
  }

  @inline def endCollection(): Unit = buffer.encoder.writeArrayEnd()


  @inline def result() = AvroPickle(buffer.result())

}
