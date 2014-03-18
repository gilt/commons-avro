package com.gilt.pickling.avroschema

import scala.pickling.{PicklingException, PBuilder, PickleTools}

final class AvroSchemaPickleBuilder(format: AvroSchemaPickleFormat, out: AvroSchemaEncodingOutput) extends PBuilder with PickleTools {
  import com.gilt.pickling.util.Types._


  private var byteBuffer: AvroSchemaEncodingOutput = out.asInstanceOf[AvroSchemaEncodingOutput]

  @inline private[this] def mkByteBuffer(): Unit =
    if (byteBuffer == null)
      byteBuffer = new AvroSchemaEncodingOutput()

  @inline def beginEntry(picklee: Any): PBuilder = withHints {
    hints =>
      mkByteBuffer()
      hints.tag.key match {
//        case KEY_INT => byteBuffer.encodeIntTo(picklee.asInstanceOf[Int])
//        case KEY_LONG => byteBuffer.encodeLongTo(picklee.asInstanceOf[Long])
//        case KEY_FLOAT => byteBuffer.encodeFloatTo(picklee.asInstanceOf[Float])
//        case KEY_DOUBLE => byteBuffer.encodeDoubleTo(picklee.asInstanceOf[Double])
//        case KEY_BOOLEAN => byteBuffer.encodeBooleanTo(picklee.asInstanceOf[Boolean])
//        case KEY_SCALA_STRING | KEY_JAVA_STRING => byteBuffer.encodeStringTo(picklee.asInstanceOf[String])
//        case KEY_BYTE => byteBuffer.encodeByteTo(picklee.asInstanceOf[Byte])
//        case KEY_SHORT => byteBuffer.encodeShortTo(picklee.asInstanceOf[Short])
//        case KEY_CHAR => byteBuffer.encodeCharTo(picklee.asInstanceOf[Char])
//        case KEY_UNIT | KEY_NULL => throw new PicklingException("Not supported yet.")
//        case KEY_ARRAY_INT => byteBuffer.encodeIntArrayTo(picklee.asInstanceOf[Array[Int]])
//        case KEY_ARRAY_LONG => byteBuffer.encodeLongArrayTo(picklee.asInstanceOf[Array[Long]])
//        case KEY_ARRAY_FLOAT => byteBuffer.encodeFloatArrayTo(picklee.asInstanceOf[Array[Float]])
//        case KEY_ARRAY_DOUBLE => byteBuffer.encodeDoubleArrayTo(picklee.asInstanceOf[Array[Double]])
//        case KEY_ARRAY_BOOLEAN => byteBuffer.encodeBooleanArrayTo(picklee.asInstanceOf[Array[Boolean]])
//        case KEY_ARRAY_BYTE => byteBuffer.encodeByteArrayTo(picklee.asInstanceOf[Array[Byte]])
//        case KEY_ARRAY_SHORT => byteBuffer.encodeShortArrayTo(picklee.asInstanceOf[Array[Short]])
//        case KEY_ARRAY_CHAR => byteBuffer.encodeCharArrayTo(picklee.asInstanceOf[Array[Char]])
//        case KEY_NIL => byteBuffer.encodeByteArrayTo(Array.empty)
//        case KEY_NONE => byteBuffer.encodeLongTo(1)
//        case key if key.startsWith(KEY_SOME) => byteBuffer.encodeLongTo(0) // TODO be nice to match against TypeRef
        case key =>
          //println(s"Unhandled begin entry: $key") //TODO remove
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
//    byteBuffer.encoder.writeArrayStart()
//    byteBuffer.encoder.setItemCount(length)
    this
  }

  @inline def putElement(pickler: PBuilder => Unit): PBuilder = {
//    byteBuffer.encoder.startItem()
    pickler(this)
    this
  }

  @inline def endCollection(): Unit = {}


  @inline def result() = AvroSchemaPickle(byteBuffer.result())

}
