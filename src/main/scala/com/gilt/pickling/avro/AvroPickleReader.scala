package com.gilt.pickling.avro

import scala.reflect.runtime.universe.{typeOf, Mirror, TypeRef, TypeTag}
import scala.reflect.runtime.{universe => ru}
import scala.pickling.{PicklingException, FastTypeTag, PReader, PickleTools}
import org.apache.avro.io.DecoderFactory
import java.io.ByteArrayInputStream
import scala.reflect.ClassTag

class AvroPickleReader(arr: Array[Byte], val mirror: Mirror, format: AvroPickleFormat) extends PReader with PickleTools {

  import format._

  //TODO be nice to used a thread local for a reuse BinaryEncoder
  private val decoder = DecoderFactory.get.directBinaryDecoder(new ByteArrayInputStream(arr), null)
  private var _lastTagRead: FastTypeTag[_] = null
  private var collectionGenericType: Option[FastTypeTag[_]] = None
  private var collectionSize: Option[Long] = None

  private val instantiableList = typeOf[::[Any]]
  private val anySymbol = typeOf[Any].typeSymbol

  private def lastTagRead: FastTypeTag[_] =
    if (_lastTagRead != null)
      _lastTagRead
    else
      throw new PicklingException("LastTagRead field is null. Should not happen")

  def beginEntryNoTag(): String = {
    withHints {
      hints =>
        collectionGenericType match {
          case None =>
            val tpe = hints.tag.tpe
            if (tpe <:< typeOf[List[_]]) {
              val genericType = determineGenericTypeOfCollection(tpe)
              val t = instantiableList.substituteSymbols(List(anySymbol), List(genericType.typeSymbol))
              val tName = t.normalize.typeSymbol.fullName + "[" + genericType.normalize.typeSymbol.fullName + "]"
              _lastTagRead = FastTypeTag(mirror, t, tName)
            } else {
              _lastTagRead = hints.tag
            }

          case Some(x) =>
            _lastTagRead = x
        }
        _lastTagRead.key
    }
  }

  def beginEntry(): FastTypeTag[_] = lastTagRead

  def atPrimitive: Boolean = primitives.contains(lastTagRead.key)

  def readPrimitive(): Any = {
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
      case KEY_UNIT | KEY_NULL => throw new PicklingException("Not supported yet.")
      case KEY_ARRAY_INT => extractToArray(decoder.readInt)
      case KEY_ARRAY_LONG => extractToArray(decoder.readLong)
      case KEY_ARRAY_FLOAT => extractToArray(decoder.readFloat)
      case KEY_ARRAY_DOUBLE => extractToArray(decoder.readDouble)
      case KEY_ARRAY_BOOLEAN => extractToArray(decoder.readBoolean)
      case KEY_ARRAY_BYTE => decoder.readBytes(null).array()
      case KEY_ARRAY_SHORT => extractToArray(() => decoder.readInt.toShort)
      case KEY_ARRAY_CHAR => extractToArray(() => decoder.readInt.toChar)
      case _ =>
        throw new PicklingException("Not supported yet.")
    }
  }

  def atObject: Boolean = !atPrimitive

  def readField(name: String): AvroPickleReader = this

  def endEntry(): Unit = {}

  def beginCollection(): PReader = {
    collectionSize = Some(decoder.readArrayStart())
    val tpe = _lastTagRead.tpe //TODO needs to be a option to be sure to be sure

    if (tpe <:< typeOf[Array[_]] || tpe <:< typeOf[List[_]] || tpe <:< typeOf[Set[_]]) {
      val t = determineGenericTypeOfCollection(tpe)
      collectionGenericType = Some(FastTypeTag(mirror, t, t.typeSymbol.fullName))
    } else
      throw new PicklingException("Collection is not supported")

    this
  }

  def readLength(): Int = {
    //TODO there maybe an issue with large Collections need to write a test to determine if it is.
    collectionSize match {
      case Some(x) => x.toInt
      case _ => throw new PicklingException("Requested collection length before beginning of collection.")
    }
  }


  def readElement(): PReader = this

  def endCollection(): Unit = {
    collectionSize = None
    collectionGenericType = None
  }

  private def extractToArray[T: ClassTag](readFunction: () => T): Array[T] = {
    val items = new scala.collection.mutable.ArrayBuffer[T]
    val numberOfItems = decoder.readArrayStart()
    (0L until numberOfItems) foreach (_ => items += readFunction())
    items.toArray
  }

  private def determineGenericTypeOfCollection(tpe: ru.Type): ru.Type = {
    tpe match {
      case TypeRef(_, _, genericType :: Nil) => genericType
      case _ => throw new PicklingException("Cannot determine generic type of collection")
    }
  }
}
