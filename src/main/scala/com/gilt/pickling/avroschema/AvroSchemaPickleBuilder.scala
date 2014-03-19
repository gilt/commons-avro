package com.gilt.pickling.avroschema

import scala.pickling._
import scala.collection.mutable.Stack
import scala.reflect.runtime.{universe => ru}
import scala.pickling.PicklingException

object AvroSchemaPickleBuilder {
  val namespace = "{\"namespace\":\"".getBytes
  val record = "\",\"type\":\"record\"".getBytes
  val recordName = ",\"name\":\"".getBytes

  val fields = "\",\"fields\":[".getBytes
  val fieldName = "{\"name\":\"".getBytes
  val fieldPeriod = "\",".getBytes

  val intField = "\"type\":\"int\"}".getBytes
  val longField = "\"type\":\"long\"}".getBytes
  val doubleField = "\"type\":\"double\"}".getBytes
  val floatField = "\"type\":\"float\"}".getBytes
  val booleanField = "\"type\":\"boolean\"}".getBytes
  val stringField = "\"type\":\"string\"}".getBytes

}

final class AvroSchemaPickleBuilder(format: AvroSchemaPickleFormat, out: AvroSchemaEncodingOutput) extends PBuilder with PickleTools {

  import com.gilt.pickling.util.Types._
  import AvroSchemaPickleBuilder._

  private var byteBuffer: AvroSchemaEncodingOutput = out.asInstanceOf[AvroSchemaEncodingOutput]
  private val tags = new Stack[FastTypeTag[_]]()

  @inline private[this] def mkByteBuffer(): Unit =
    if (byteBuffer == null)
      byteBuffer = new AvroSchemaEncodingOutput()

  @inline def beginEntry(picklee: Any): PBuilder = withHints {
    hints =>
      mkByteBuffer()
      hints.tag.key match {
        case KEY_INT => byteBuffer.put(intField)
        case KEY_LONG => byteBuffer.put(longField)
        case KEY_FLOAT => byteBuffer.put(floatField)
        case KEY_DOUBLE => byteBuffer.put(doubleField)
        case KEY_BOOLEAN => byteBuffer.put(booleanField)
        case KEY_SCALA_STRING | KEY_JAVA_STRING => byteBuffer.put(stringField)
        case KEY_BYTE => byteBuffer.put(intField)
        case KEY_SHORT => byteBuffer.put(intField)
        case KEY_CHAR => byteBuffer.put(intField)
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
          processObject(hints)
      }
      this
  }

  @inline def putField(name: String, pickler: PBuilder => Unit): PBuilder = {
    byteBuffer.put(fieldName)
    byteBuffer.put(name.getBytes)
    byteBuffer.put(fieldPeriod)
    pickler(this)
    this
  }

  @inline def endEntry(): Unit =
    if (tags.length == 0)
      byteBuffer.put("]}".getBytes)
    else
      tags.pop()

  @inline def beginCollection(length: Int): PBuilder = this

  @inline def putElement(pickler: PBuilder => Unit): PBuilder = this

  @inline def endCollection(): Unit = {}

  @inline def result() = AvroSchemaPickle(byteBuffer.result())

  private def processObject(hints: Hints) =
    if (tags.length == 0)
      processRootObject(hints)
    else
      throw new PicklingException("Not supporting nested objects yet.")

  private def processRootObject(hints: Hints): AvroSchemaEncodingOutput = {
    val typeSymbol = hints.tag.tpe.typeSymbol
    if (typeSymbol.isClass && typeSymbol.asClass.isCaseClass) {
      tags.push(hints.tag)
      addPreamable(typeSymbol)
    } else {
      throw new PicklingException("Only case classes are supported as root objects")
    }
  }

  private def addPreamable(typeSymbol: ru.Symbol): AvroSchemaEncodingOutput = {
    byteBuffer.put(namespace)
    byteBuffer.put(typeSymbol.owner.fullName.getBytes)
    byteBuffer.put(record)
    byteBuffer.put(recordName)
    byteBuffer.put(typeSymbol.name.decoded.getBytes)
    byteBuffer.put(fields)
  }
}
