package com.gilt.pickling

import java.util.UUID

object TestObjs {
  case class VectorContainer(value: Vector[Int])
  case class MapContainer(value: Map[Int, Int])
  case class SeqContainer(value: Seq[Int])

  case class SingleInt(id: Int)
  case class SingleLong(id: Long)
  case class SingleDouble(id: Double)
  case class SingleFloat(id: Float)
  case class SingleBoolean(id: Boolean)
  case class SingleString(id: String)
  case class SingleByte(id: Byte)
  case class SingleShort(id: Short)
  case class SingleChar(id: Char)

  case class ArrayOfInts(list: Array[Int])
  case class ArrayOfLongs(list: Array[Long])
  case class ArrayOfDoubles(list: Array[Double])
  case class ArrayOfFloats(list: Array[Float])
  case class ArrayOfBooleans(list: Array[Boolean])
  case class ArrayOfStrings(list: Array[String])
  case class ArrayOfBytes(list: Array[Byte])
  case class ArrayOfShorts(list: Array[Short])
  case class ArrayOfChars(list: Array[Char])

  case class ListOfInts(list: List[Int])
  case class ListOfLongs(list: List[Long])
  case class ListOfDoubles(list: List[Double])
  case class ListOfFloats(list: List[Float])
  case class ListOfBooleans(list: List[Boolean])
  case class ListOfStrings(list: List[String])
  case class ListOfBytes(list: List[Byte])
  case class ListOfShorts(list: List[Short])
  case class ListOfChars(list: List[Char])

  case class SetOfInts(list: Set[Int])
  case class SetOfLongs(list: Set[Long])
  case class SetOfDoubles(list: Set[Double])
  case class SetOfFloats(list: Set[Float])
  case class SetOfBooleans(list: Set[Boolean])
  case class SetOfStrings(list: Set[String])
  case class SetOfBytes(list: Set[Byte])
  case class SetOfShorts(list: Set[Short])
  case class SetOfChars(list: Set[Char])

  case class MapOfInts(list: Map[String, Int])
  case class MapOfLongs(list: Map[String, Long])
  case class MapOfDoubles(list: Map[String, Double])
  case class MapOfFloats(list: Map[String, Float])
  case class MapOfBooleans(list: Map[String, Boolean])
  case class MapOfStrings(list: Map[String, String])
  case class MapOfBytes(list: Map[String, Byte])
  case class MapOfShorts(list: Map[String, Short])
  case class MapOfChars(list: Map[String, Char])

  case class MultipleField(intField: Int,
                           longField: Long,
                           doubleField: Double,
                           floatField: Float,
                           booleanField: Boolean,
                           stringField: String,
                           byteField: Byte,
                           shortField: Short,
                           charField: Char)

  case class SingleOptionInt(id: Option[Int])
  case class SingleOptionLong(id: Option[Long])
  case class SingleOptionDouble(id: Option[Double])
  case class SingleOptionFloat(id: Option[Float])
  case class SingleOptionBoolean(id: Option[Boolean])
  case class SingleOptionString(id: Option[String])
  case class SingleOptionByte(id: Option[Byte])
  case class SingleOptionShort(id: Option[Short])
  case class SingleOptionChar(id: Option[Char])

  case class OptionArrayOfInts(list: Option[Array[Int]])
  case class OptionArrayOfLongs(list: Option[Array[Long]])
  case class OptionArrayOfDoubles(list: Option[Array[Double]])
  case class OptionArrayOfFloats(list: Option[Array[Float]])
  case class OptionArrayOfBooleans(list: Option[Array[Boolean]])
  case class OptionArrayOfStrings(list: Option[Array[String]])
  case class OptionArrayOfBytes(list: Option[Array[Byte]])
  case class OptionArrayOfShorts(list: Option[Array[Short]])
  case class OptionArrayOfChars(list: Option[Array[Char]])

  case class OptionListOfInts(list: Option[List[Int]])
  case class OptionListOfLongs(list: Option[List[Long]])
  case class OptionListOfDoubles(list: Option[List[Double]])
  case class OptionListOfFloats(list: Option[List[Float]])
  case class OptionListOfBooleans(list: Option[List[Boolean]])
  case class OptionListOfStrings(list: Option[List[String]])
  case class OptionListOfBytes(list: Option[List[Byte]])
  case class OptionListOfShorts(list: Option[List[Short]])
  case class OptionListOfChars(list: Option[List[Char]])

  case class OptionSetOfInts(list: Option[Set[Int]])
  case class OptionSetOfLongs(list: Option[Set[Long]])
  case class OptionSetOfDoubles(list: Option[Set[Double]])
  case class OptionSetOfFloats(list: Option[Set[Float]])
  case class OptionSetOfBooleans(list: Option[Set[Boolean]])
  case class OptionSetOfStrings(list: Option[Set[String]])
  case class OptionSetOfBytes(list: Option[Set[Byte]])
  case class OptionSetOfShorts(list: Option[Set[Short]])
  case class OptionSetOfChars(list: Option[Set[Char]])

  case class OptionMapOfIntInts(list: Option[Map[Int, Int]])
  case class MapOfIntInts(list: Map[Int, Int])

  case class OptionMapOfInts(list: Option[Map[String, Int]])
  case class OptionMapOfLongs(list: Option[Map[String, Long]])
  case class OptionMapOfDoubles(list: Option[Map[String, Double]])
  case class OptionMapOfFloats(list: Option[Map[String, Float]])
  case class OptionMapOfBooleans(list: Option[Map[String, Boolean]])
  case class OptionMapOfStrings(list: Option[Map[String, String]])
  case class OptionMapOfBytes(list: Option[Map[String, Byte]])
  case class OptionMapOfShorts(list: Option[Map[String, Short]])
  case class OptionMapOfChars(list: Option[Map[String, Char]])

  case class InnerObject(id: Int)
  case class SingleObject(inner:InnerObject)
  case class SingleOptionObject(inner: Option[InnerObject])
  case class ListOfObjects(list: List[InnerObject])
  case class ArrayOfObjects(list: Array[InnerObject])
  case class SetOfObjects(list: Set[InnerObject])
  case class MapOfObjects(list: Map[String, InnerObject])
  case class SelfReferencingObject(id: Int, inner:Option[SelfReferencingObject])
  case class MultipleSameObject(first: InnerObject, second: InnerObject)
  case class SingleUuid(uuid: UUID)
  case class SingleOptionUuid(uuid: Option[UUID])
  case class MultipleUuid(uuid1: UUID, uuid2: UUID)
  case class SingleBigDecimal(big: BigDecimal)
  case class MultipleBigDecimal(big1: BigDecimal, big2: BigDecimal)

  object ObjInnerObject{ val id = 1}
  class ClassInnerObject(id: Int)
}
