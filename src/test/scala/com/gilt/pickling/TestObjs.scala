package com.gilt.pickling

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

  case class MultipleField(intField: Int,
                           longField: Long,
                           doubleField: Double,
                           floatField: Float,
                           booleanField: Boolean,
                           stringField: String,
                           byteField: Byte,
                           shortField: Short,
                           charField: Char)

  case class SingleOpInt(id: Option[Int])
  case class SingleOpLong(id: Option[Long])
  case class SingleOpDouble(id: Option[Double])
  case class SingleOpFloat(id: Option[Float])
  case class SingleOpBoolean(id: Option[Boolean])
  case class SingleOpString(id: Option[String])
  case class SingleOpByte(id: Option[Byte])
  case class SingleOpShort(id: Option[Short])
  case class SingleOpChar(id: Option[Char])

  case class OpArrayOfInts(list: Option[Array[Int]])
  case class OpArrayOfLongs(list: Option[Array[Long]])
  case class OpArrayOfDoubles(list: Option[Array[Double]])
  case class OpArrayOfFloats(list: Option[Array[Float]])
  case class OpArrayOfBooleans(list: Option[Array[Boolean]])
  case class OpArrayOfStrings(list: Option[Array[String]])
  case class OpArrayOfBytes(list: Option[Array[Byte]])
  case class OpArrayOfShorts(list: Option[Array[Short]])
  case class OpArrayOfChars(list: Option[Array[Char]])

  case class OpListOfInts(list: Option[List[Int]])
  case class OpListOfLongs(list: Option[List[Long]])
  case class OpListOfDoubles(list: Option[List[Double]])
  case class OpListOfFloats(list: Option[List[Float]])
  case class OpListOfBooleans(list: Option[List[Boolean]])
  case class OpListOfStrings(list: Option[List[String]])
  case class OpListOfBytes(list: Option[List[Byte]])
  case class OpListOfShorts(list: Option[List[Short]])
  case class OpListOfChars(list: Option[List[Char]])

  case class OpSetOfInts(list: Option[Set[Int]])
  case class OpSetOfLongs(list: Option[Set[Long]])
  case class OpSetOfDoubles(list: Option[Set[Double]])
  case class OpSetOfFloats(list: Option[Set[Float]])
  case class OpSetOfBooleans(list: Option[Set[Boolean]])
  case class OpSetOfStrings(list: Option[Set[String]])
  case class OpSetOfBytes(list: Option[Set[Byte]])
  case class OpSetOfShorts(list: Option[Set[Short]])
  case class OpSetOfChars(list: Option[Set[Char]])
}
