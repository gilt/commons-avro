package com.gilt.pickling.avro

import scala.pickling._
import scala.reflect.runtime.universe.{TypeRef, Type, ClassSymbol}
import scala.pickling.PicklingException
import scala.collection.mutable

final class AvroPickleBuilder(format: AvroPickleFormat, buffer: AvroEncodingOutput = new AvroEncodingOutput()) extends PBuilder with PickleTools {

  import com.gilt.pickling.util.Types._

  private val tags = new mutable.Stack[Type]()

  @inline def beginEntry(picklee: Any): PBuilder = withHints {
    hints =>
      (hints.tag.tpe, hints.tag.key) match {
        case (tpe, KEY_INT) if isNotRootObject => buffer.encodeIntTo(picklee.asInstanceOf[Int])
        case (_, KEY_LONG) if isNotRootObject => buffer.encodeLongTo(picklee.asInstanceOf[Long])
        case (_, KEY_FLOAT) if isNotRootObject => buffer.encodeFloatTo(picklee.asInstanceOf[Float])
        case (_, KEY_DOUBLE) if isNotRootObject => buffer.encodeDoubleTo(picklee.asInstanceOf[Double])
        case (_, KEY_BOOLEAN) if isNotRootObject => buffer.encodeBooleanTo(picklee.asInstanceOf[Boolean])
        case (_, KEY_SCALA_STRING) | (_, KEY_JAVA_STRING) if isNotRootObject => buffer.encodeStringTo(picklee.asInstanceOf[String])
        case (_, KEY_BYTE) if isNotRootObject => buffer.encodeByteTo(picklee.asInstanceOf[Byte])
        case (_, KEY_SHORT) if isNotRootObject=> buffer.encodeShortTo(picklee.asInstanceOf[Short])
        case (_, KEY_CHAR) if isNotRootObject=> buffer.encodeCharTo(picklee.asInstanceOf[Char])
        case (_, KEY_ARRAY_INT) if parentIsACaseClass => buffer.encodeIntArrayTo(picklee.asInstanceOf[Array[Int]])
        case (_, KEY_ARRAY_LONG) if parentIsACaseClass => buffer.encodeLongArrayTo(picklee.asInstanceOf[Array[Long]])
        case (_, KEY_ARRAY_FLOAT) if parentIsACaseClass => buffer.encodeFloatArrayTo(picklee.asInstanceOf[Array[Float]])
        case (_, KEY_ARRAY_DOUBLE) if parentIsACaseClass=> buffer.encodeDoubleArrayTo(picklee.asInstanceOf[Array[Double]])
        case (_, KEY_ARRAY_BOOLEAN) if parentIsACaseClass => buffer.encodeBooleanArrayTo(picklee.asInstanceOf[Array[Boolean]])
        case (_, KEY_ARRAY_BYTE) if parentIsACaseClass => buffer.encodeByteArrayTo(picklee.asInstanceOf[Array[Byte]])
        case (_, KEY_ARRAY_SHORT) if parentIsACaseClass=> buffer.encodeShortArrayTo(picklee.asInstanceOf[Array[Short]])
        case (_, KEY_ARRAY_CHAR) if parentIsACaseClass=> buffer.encodeCharArrayTo(picklee.asInstanceOf[Array[Char]])
        case (_, KEY_NIL) if parentIsACaseClass => buffer.encodeByteArrayTo(Array.empty)
        case (_, KEY_NONE) if parentIsACaseClass => buffer.encodeLongTo(0)
        case (tpe, _) if tpe <:< someType && parentIsACaseClass => buffer.encodeLongTo(1)
        case (tpe, _) if (tpe <:< iterableType || tpe <:< arrayType) && parentIsACaseClass =>
        case (tpe@TypeRef(_, s:ClassSymbol, _), _) if s.isCaseClass && !(tpe <:< iterableType) =>
        case (_, KEY_UNIT) | (_, KEY_NULL) => throw new PicklingException("Not supported.")
        case (_, key) => throw new PicklingException(s"$key is not supported.")
      }
      tags.push(hints.tag.tpe)
      this
  }

  @inline def putField(name: String, pickler: PBuilder => Unit): PBuilder = {
    pickler(this)
    this
  }

  @inline def endEntry(): Unit = tags.pop()

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

  private def isNotRootObject: Boolean = tags.length > 0

  private def parentIsACaseClass: Boolean =
    tags.elems match {
      case TypeRef(_, s:ClassSymbol, _) :: tail if s.isCaseClass => true
      case _ => false
    }

}
