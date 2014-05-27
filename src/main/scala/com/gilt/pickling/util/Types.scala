package com.gilt.pickling.util

import scala.pickling.FastTypeTag

object Types {
  val KEY_UUID = "java.util.UUID"
  val KEY_SOME = "scala.Some"
  val KEY_OPTION = "scala.Option"
  val KEY_NONE = "scala.None.type"

  val KEY_MAP = "scala.collection.immutable.Map[java.lang.String,"
  val KEY_ARRAY = "scala.Array"
  val KEY_SET = "scala.collection.immutable.Set"
  val KEY_SEQ = "scala.collection.Seq"
  val KEY_VECTOR = "scala.collection.immutable.Vector"

  val KEY_LIST = "scala.collection.immutable.List"
  val KEY_LIST_COLON_COLON = "scala.collection.immutable.$colon$colon"
  val KEY_NIL = "scala.collection.immutable.Nil.type"

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

  val primitives = Set(KEY_NULL, KEY_BYTE, KEY_SHORT, KEY_CHAR, KEY_INT, KEY_LONG, KEY_BOOLEAN, KEY_FLOAT,
    KEY_DOUBLE, KEY_UNIT, KEY_SCALA_STRING, KEY_JAVA_STRING, KEY_ARRAY_BYTE, KEY_ARRAY_SHORT, KEY_ARRAY_CHAR,
    KEY_ARRAY_INT, KEY_ARRAY_LONG, KEY_ARRAY_BOOLEAN, KEY_ARRAY_FLOAT, KEY_ARRAY_DOUBLE)

  val supportedCollections = Set(KEY_MAP, KEY_ARRAY, KEY_SET, KEY_SEQ, KEY_LIST, KEY_VECTOR, KEY_LIST_COLON_COLON, KEY_NIL)

  // The use of this cache makes the assumption that the number of case classes will be bounded. 
  private val isCaseClassResultsCache = scala.collection.mutable.Map[String, Boolean]()
  // This is synchronized due to a scala 2.10 reflection bug.
  private def synchronizedIsCaseClass(tag: FastTypeTag[_]) : Boolean = Types.synchronized(tag.tpe.typeSymbol.isClass && tag.tpe.typeSymbol.asClass.isCaseClass)

  def isPrimitive(tag: FastTypeTag[_]): Boolean = primitives.contains(tag.key)
  def isTypeOf(tag: FastTypeTag[_], baseType: String): Boolean =  tag.key.startsWith(baseType)
//  def isCaseClass(tag: FastTypeTag[_]) : Boolean = isCaseClassResultsCache.getOrElseUpdate(tag.key, synchronizedIsCaseClass(tag))
  def isCaseClass(tag: FastTypeTag[_]) : Boolean = Types.synchronized(tag.tpe.typeSymbol.isClass && tag.tpe.typeSymbol.asClass.isCaseClass)
  def isSupportedCollectionType(tag: FastTypeTag[_]): Boolean = supportedCollections.exists(collectionType => isTypeOf(tag, collectionType))
}
