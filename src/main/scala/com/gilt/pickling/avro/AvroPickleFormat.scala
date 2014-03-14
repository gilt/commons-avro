package com.gilt.pickling.avro

import scala.pickling.{PBuilder, FastTypeTag, PickleFormat}
import scala.reflect.runtime.universe.Mirror

class AvroPickleFormat extends PickleFormat {
  val KEY_NULL = FastTypeTag.Null.key
  val KEY_UNIT = FastTypeTag.Unit.key

  val KEY_INT = FastTypeTag.Int.key
  val KEY_LONG = FastTypeTag.Long.key
  val KEY_FLOAT = FastTypeTag.Float.key
  val KEY_DOUBLE = FastTypeTag.Double.key
  val KEY_BOOLEAN = FastTypeTag.Boolean.key
  val KEY_BYTE = FastTypeTag.Byte.key
  val KEY_SHORT = FastTypeTag.Short.key

  val KEY_CHAR = FastTypeTag.Char.key

  val KEY_SCALA_STRING = FastTypeTag.ScalaString.key
  val KEY_JAVA_STRING = FastTypeTag.JavaString.key

  val KEY_ARRAY_INT = FastTypeTag.ArrayInt.key
  val KEY_ARRAY_LONG = FastTypeTag.ArrayLong.key
  val KEY_ARRAY_FLOAT = FastTypeTag.ArrayFloat.key
  val KEY_ARRAY_DOUBLE = FastTypeTag.ArrayDouble.key
  val KEY_ARRAY_BOOLEAN = FastTypeTag.ArrayBoolean.key
  val KEY_ARRAY_BYTE = FastTypeTag.ArrayByte.key
  val KEY_ARRAY_SHORT = FastTypeTag.ArrayShort.key
  val KEY_ARRAY_CHAR = FastTypeTag.ArrayChar.key

  val KEY_REF = FastTypeTag.Ref.key

  val primitives = Set(KEY_NULL, KEY_REF, KEY_BYTE, KEY_SHORT, KEY_CHAR, KEY_INT, KEY_LONG, KEY_BOOLEAN, KEY_FLOAT,
    KEY_DOUBLE, KEY_UNIT, KEY_SCALA_STRING, KEY_JAVA_STRING, KEY_ARRAY_BYTE, KEY_ARRAY_SHORT, KEY_ARRAY_CHAR,
    KEY_ARRAY_INT, KEY_ARRAY_LONG, KEY_ARRAY_BOOLEAN, KEY_ARRAY_FLOAT, KEY_ARRAY_DOUBLE)

  val nullablePrimitives = Set()

  type PickleType = AvroPickle
  type OutputType = AvroEncodingOutput

  def createBuilder() =
    new AvroPickleBuilder(this, null)

  def createBuilder(out: AvroEncodingOutput): PBuilder =
    new AvroPickleBuilder(this, out)

  def createReader(pickle: PickleType, mirror: Mirror) =
    new AvroPickleReader(pickle.value, mirror, this)

}
