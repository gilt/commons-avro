package com.gilt.pickling.util

import scala.pickling.FastTypeTag
import scala.reflect.runtime.universe.typeOf
import scala.Some

object Types {
  val someType = typeOf[Some[Any]]
  val iterableType = typeOf[Iterable[_]]
  val arrayType = typeOf[Array[_]]
  val listType = typeOf[List[Any]]
  val optionType = typeOf[Option[_]]

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

  val KEY_NIL = "scala.collection.immutable.Nil.type"
  val KEY_NONE = "scala.None.type"

  val primitives = Set(KEY_NULL, KEY_BYTE, KEY_SHORT, KEY_CHAR, KEY_INT, KEY_LONG, KEY_BOOLEAN, KEY_FLOAT,
    KEY_DOUBLE, KEY_UNIT, KEY_SCALA_STRING, KEY_JAVA_STRING, KEY_ARRAY_BYTE, KEY_ARRAY_SHORT, KEY_ARRAY_CHAR,
    KEY_ARRAY_INT, KEY_ARRAY_LONG, KEY_ARRAY_BOOLEAN, KEY_ARRAY_FLOAT, KEY_ARRAY_DOUBLE)
}
