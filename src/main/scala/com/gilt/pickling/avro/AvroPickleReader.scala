package com.gilt.pickling.avro

import scala.pickling._
import org.apache.avro.io.DecoderFactory
import java.io.ByteArrayInputStream
import scala.reflect.ClassTag
import scala.collection.mutable
import scala.reflect.runtime.universe.Mirror
import scala.pickling.PicklingException
import com.gilt.pickling.util.Types
import java.nio.ByteBuffer

class AvroPickleReader(arr: Array[Byte], val mirror: Mirror, format: AvroPickleFormat) extends PReader with PickleTools {

  import Types._

  //TODO Arrays and Maps are read in Blocks
  //TODO List[Byte] should write avro bytes than array of byte
  //TODO be nice to used a thread local for a reuse BinaryEncoder
  private val decoder = DecoderFactory.get.directBinaryDecoder(new ByteArrayInputStream(arr), null)
  private val tags = new mutable.Stack[FastTypeTag[_]]()

  private var collectionGenericType: Option[FastTypeTag[_]] = None
  private var lastTagReadOption: Option[FastTypeTag[_]] = None
  private var collectionSize: Option[Long] = None

  def beginEntryNoTag(): String = beginEntry().key

  def beginEntry(): FastTypeTag[_] = {
    withHints {
      hints =>
        val nextTag = determineNextTag(hints.tag)
        lastTagReadOption = Some(nextTag)
        tags.push(nextTag)
        lastTagRead
    }
  }

  def endEntry(): Unit = tags.pop()

  def atPrimitive: Boolean = isPrimitive(lastTagRead)

  def atObject: Boolean = !atPrimitive

  def readPrimitive(): Any =
    lastTagRead.key match {
      case KEY_INT => decoder.readInt
      case KEY_LONG => decoder.readLong
      case KEY_FLOAT => decoder.readFloat
      case KEY_DOUBLE => decoder.readDouble
      case KEY_BOOLEAN => decoder.readBoolean
      case KEY_SCALA_STRING | KEY_JAVA_STRING => decoder.readString()
      case KEY_BYTE => decoder.readInt.toByte
      case KEY_SHORT => decoder.readInt.toShort
      case KEY_CHAR => decoder.readInt.toChar
      case KEY_UNIT | KEY_NULL => throw new PicklingException("Not supported - unit or null.")
      case KEY_ARRAY_INT => extractToArray(decoder.readInt)
      case KEY_ARRAY_LONG => extractToArray(decoder.readLong)
      case KEY_ARRAY_FLOAT => extractToArray(decoder.readFloat)
      case KEY_ARRAY_DOUBLE => extractToArray(decoder.readDouble)
      case KEY_ARRAY_BOOLEAN => extractToArray(decoder.readBoolean)
      case KEY_ARRAY_BYTE if tags.elems.take(2).map(_.key) == Seq(KEY_ARRAY_BYTE, KEY_UUID) => extractFixedLengthArray // bytes to generate UUID
      case KEY_ARRAY_BYTE => decoder.readBytes(null).array()
      case KEY_ARRAY_SHORT => extractToArray(() => decoder.readInt.toShort)
      case KEY_ARRAY_CHAR => extractToArray(() => decoder.readInt.toChar)
      case _ => throw new PicklingException("Not supported - unknown.")
    }

  def readField(name: String): AvroPickleReader = this

  def beginCollection(): PReader = withHints {
    hints =>
      collectionSize = determineCollectionSizeFromLastTag()
      collectionGenericType = determineGenericTypeConvertToTag(lastTagRead)
      this
  }

  private def determineCollectionSizeFromLastTag(): Some[Long] =
    lastTagRead match {
      case tag if isTypeOf(tag, KEY_MAP) => Some(decoder.readMapStart())
      case tag if isSupportedCollectionType(tag) => Some(decoder.readArrayStart())
      case _ => throw new PicklingException("Collection is not supported")
    }

  def readLength(): Int =
    collectionSize match {
      case Some(x) => x.toInt
      case _ => throw new PicklingException("Requested collection length before beginning of collection.")
    }

  def readElement(): PReader = {
    collectionGenericType match {
      case Some(tag) =>
        tags.push(tag)
        hintTag(tag)
      case _ => throw new PicklingException("CollectionGenericType field is null. Should not happen")
    }
    this
  }

  def endCollection(): Unit = {
    collectionSize.foreach { size =>
      if (size > 0) {
        tags.pop()
        decoder.readBytes(ByteBuffer.wrap(new Array(1)))
      }
    }

    collectionSize = None
    collectionGenericType = None
  }

  private def lastTagRead: FastTypeTag[_] =
    lastTagReadOption match {
      case Some(x) => x
      case _ => throw new PicklingException("LastTagRead field is null. Should not happen")
    }

  private def determineNextTag(fastTypeTag: FastTypeTag[_]): FastTypeTag[_] =
    fastTypeTag match {
      case tag if isPrimitive(tag) && isNotRootObject => tag // A Primitive type
      case tag if isTypeOf(tag, KEY_UUID) && isNotRootObject => tag // A UUID type
      case tag if isTypeOf(tag, KEY_SCALA_BIG_DECIMAL) || isTypeOf(tag, KEY_JAVA_BIG_DECIMAL) || isTypeOf(tag, KEY_MATH_CONTEXT) || isTypeOf(tag, KEY_DATETIME) && isNotRootObject => tag // A BigDecimal type
      case tag if (isTypeOf(tag, KEY_SCALA_STRING) || isTypeOf(tag, KEY_JAVA_STRING)) && isNotRootObject => tag // A String type
      case tag if isTypeOf(tag, KEY_LIST) && isNotRootObject => buildFastTypeTagWithInstantiableList(tag) // Handles the case that List does not have an empty constructor
      case tag if isSupportedCollectionType(tag) && !isTypeOf(tag, KEY_LIST) && isNotRootObject => tag // A Iteration or Array type.
      case tag if isTypeOf(tag, KEY_OPTION) && isNotRootObject => buildFastTypeTagFromOption(tag) // Handles the case where the next type is an option
      case tag if !isSupportedCollectionType(tag) && isCaseClass(tag) => tag // A Case Class type
      case tag => throw new PicklingException(s"$tag is not supported")
    }

  private def buildFastTypeTagFromOption(tag: FastTypeTag[_]): FastTypeTag[_] =
    decoder.readLong() match {
      case 1L => FastTypeTag(s"$KEY_SOME[${extractGenericTypeFromTag(tag)}]")
      case 0L => FastTypeTag(KEY_NONE)
      case _ => throw new PicklingException("Corrupted input. Unable to determine status of option")
    }

  private def buildFastTypeTagWithInstantiableList(tag: FastTypeTag[_]): FastTypeTag[_] = FastTypeTag(s"$KEY_LIST_COLON_COLON[${extractGenericTypeFromTag(tag)}]")

  private def extractGenericTypeFromTag(tag: FastTypeTag[_]): String = tag.key.substring(tag.key.indexOf('[') + 1, tag.key.length - 1)

  private def extractToArray[T: ClassTag](readFunction: () => T): Array[T] = {
    val items = new scala.collection.mutable.ArrayBuffer[T]
    val numberOfItems = decoder.readArrayStart()
    (0L until numberOfItems) foreach (_ => items += readFunction())
    if (numberOfItems > 0) decoder.readBytes(ByteBuffer.wrap(new Array(1)))
    items.toArray
  }

  private def determineGenericTypeConvertToTag(tag: FastTypeTag[_]): Option[FastTypeTag[_]] = Some(FastTypeTag(extractGenericTypeFromTag(tag)))

  private def isNotRootObject: Boolean = tags.length > 0

  private def extractFixedLengthArray: Array[Byte] = {
    val uuidInBytes = new Array[Byte](16)
    decoder.readFixed(uuidInBytes)
    uuidInBytes
  }
}
