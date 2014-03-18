package com.gilt.pickling.avro

import scala.reflect.runtime.universe.{typeOf, Mirror, TypeRef}
import scala.reflect.runtime.{universe => ru}
import scala.pickling.{PicklingException, FastTypeTag, PReader, PickleTools}
import org.apache.avro.io.DecoderFactory
import java.io.ByteArrayInputStream
import scala.reflect.ClassTag
import com.gilt.pickling.util.Tools._

class AvroPickleReader(arr: Array[Byte], val mirror: Mirror, format: AvroPickleFormat) extends PReader with PickleTools {
  import com.gilt.pickling.util.Types._

  //TODO be nice to used a thread local for a reuse BinaryEncoder
  private val decoder = DecoderFactory.get.directBinaryDecoder(new ByteArrayInputStream(arr), null)
  private var _lastTagRead: FastTypeTag[_] = null

  private var collectionGenericType: Option[FastTypeTag[_]] = None
  private var collectionSize: Option[Long] = None

  private val listType = typeOf[List[Any]]
  private val someType = typeOf[Some[Any]]
  private val optionType = typeOf[Option[Any]]
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
            if (tpe <:< listType)
              _lastTagRead = buildFastTypeTagWithInstantiableList(tpe) //handles the case that List does not have an empty constructor
            else if (tpe <:< optionType)
              _lastTagRead = buildFastTypeTagFromOption(tpe)
            else
              _lastTagRead = hints.tag
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

    if (tpe <:< typeOf[Array[_]] || tpe <:< typeOf[Iterable[_]]) {
      val t = determineGenericType(tpe)
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

  private def buildFastTypeTagFromOption(tpe: ru.Type): FastTypeTag[_] =
    decoder.readLong() match {
      case 0L => buildSomeFastTypeTagFromOption(tpe)
      case 1L => FastTypeTag(mirror, KEY_NONE)
      case _ => throw new PicklingException("Corrupted input. Unable to determine status of option")
    }

  private def buildSomeFastTypeTagFromOption(tpe: ru.Type): FastTypeTag[_] = {
    val optionType = subsituteAnyInTypeWith(someType, determineGenericType(tpe))
    FastTypeTag(mirror, optionType, optionType.key)
  }

  private def buildFastTypeTagWithInstantiableList(tpe: ru.Type): FastTypeTag[_] = {
    val listType = subsituteAnyInTypeWith(instantiableList, determineGenericType(tpe))
    FastTypeTag(mirror, listType, listType.key)
  }

  private def subsituteAnyInTypeWith(rootType: ru.Type, to: ru.Type): ru.Type =
    rootType.map {
      t => if (t.typeSymbol == anySymbol) to else t
    }

  private def extractToArray[T: ClassTag](readFunction: () => T): Array[T] = {
    val items = new scala.collection.mutable.ArrayBuffer[T]
    val numberOfItems = decoder.readArrayStart()
    (0L until numberOfItems) foreach (_ => items += readFunction())
    items.toArray
  }

  private def determineGenericType(tpe: ru.Type): ru.Type = {
    tpe match {
        // this only supports collections with a single type, so, not Map for example,
        // which returns a list of length 2 in the third position.
      case TypeRef(_, _, genericType :: Nil) => genericType
      case _ => throw new PicklingException(s"Cannot determine generic type of collection ($tpe)")
    }
  }
}
