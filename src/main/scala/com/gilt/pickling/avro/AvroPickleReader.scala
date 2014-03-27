package com.gilt.pickling.avro

import scala.reflect.runtime.universe.{typeOf, Mirror, TypeRef, Type, ClassSymbol}
import scala.pickling.{PicklingException, FastTypeTag, PReader, PickleTools}
import org.apache.avro.io.DecoderFactory
import java.io.ByteArrayInputStream
import scala.reflect.ClassTag
import com.gilt.pickling.util.Tools._
import scala.collection.mutable

object AvroPickleReader {
  private val instantiableList = typeOf[::[Any]]
  private val anySymbol = typeOf[Any].typeSymbol
  private val stringType = typeOf[String]
}

class AvroPickleReader(arr: Array[Byte], val mirror: Mirror, format: AvroPickleFormat) extends PReader with PickleTools {

  import com.gilt.pickling.util.Types._
  import AvroPickleReader._

  //TODO Arrays and Maps are read in Blocks
  //TODO List[Byte] should write avro bytes than array of byte
  //TODO be nice to used a thread local for a reuse BinaryEncoder
  private val decoder = DecoderFactory.get.directBinaryDecoder(new ByteArrayInputStream(arr), null)
  private val tags = new mutable.Stack[Type]()

  private var collectionGenericType: Option[FastTypeTag[_]] = None
  private var lastTagReadOption: Option[FastTypeTag[_]] = None
  private var collectionSize: Option[Long] = None

  def beginEntryNoTag(): String = beginEntry().key

  def beginEntry(): FastTypeTag[_] = {
    withHints {
      hints =>
        val nextTag = determineNextTag(hints.tag)
        lastTagReadOption = Some(nextTag)
        tags.push(nextTag.tpe)
        lastTagRead
    }
  }

  def endEntry(): Unit = tags.pop()

  def atPrimitive: Boolean = primitives.contains(lastTagRead.key)

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
      case KEY_ARRAY_BYTE => decoder.readBytes(null).array()
      case KEY_ARRAY_SHORT => extractToArray(() => decoder.readInt.toShort)
      case KEY_ARRAY_CHAR => extractToArray(() => decoder.readInt.toChar)
      case _ => throw new PicklingException("Not supported - unknown.")
    }

  def readField(name: String): AvroPickleReader = this

  def beginCollection(): PReader = withHints {
    hints =>
      lastTagRead.tpe match {
        case t: TypeRef if t <:< arrayType || t <:< iterableType =>
          collectionSize = Some(decoder.readArrayStart())
          collectionGenericType = determineGenericTypeConvertToTag(t)
        case t: TypeRef if t <:< mapType =>
          collectionSize = Some(decoder.readMapStart())
          collectionGenericType = determineGenericTypeConvertToTag(t)
        case _ => throw new PicklingException("Collection is not supported")
      }
      this
  }

  def readLength(): Int =
    collectionSize match {
      case Some(x) => x.toInt
      case _ => throw new PicklingException("Requested collection length before beginning of collection.")
    }

  def readElement(): PReader = {
    tags.push(collectionGenericType.get.tpe)
    hintTag(collectionGenericType.get) //TODO a get!!!
    this
  }

  def endCollection(): Unit = {
    collectionSize.foreach(s => if (s > 0) tags.pop())
    collectionSize = None
    collectionGenericType = None
  }

  private def lastTagRead: FastTypeTag[_] =
    lastTagReadOption match {
      case Some(x) => x
      case _ => throw new PicklingException("LastTagRead field is null. Should not happen")
    }

  private def determineNextTag(tag: FastTypeTag[_]): FastTypeTag[_] =
    tag.tpe match {
      case t: TypeRef if t.isEffectivelyPrimitive && isNotRootObject => tag
      case t: TypeRef if (t <:< FastTypeTag.ScalaString.tpe || t <:< FastTypeTag.JavaString.tpe) && isNotRootObject => tag
      case t: TypeRef if t <:< listType && parentIsACaseClassOrOption => buildFastTypeTagWithInstantiableList(t) //handles the case that List does not have an empty constructor
      case t: TypeRef if (t <:< iterableType || t <:< arrayType) && !(t <:< listType) && parentIsACaseClassOrOption => tag
      case t: TypeRef if t <:< optionType && parentIsACaseClass => buildFastTypeTagFromOption(t)
      case t@TypeRef(_, s: ClassSymbol, _) if s.isCaseClass && !(t <:< iterableType) => tag
      case t => throw new PicklingException(s"$t is not supported")
    }

  private def buildFastTypeTagFromOption(tpe: Type): FastTypeTag[_] =
    decoder.readLong() match {
      case 1L => buildSomeFastTypeTagFromOption(tpe)
      case 0L => FastTypeTag(mirror, KEY_NONE)
      case _ => throw new PicklingException("Corrupted input. Unable to determine status of option")
    }

  private def buildSomeFastTypeTagFromOption(tpe: Type): FastTypeTag[_] = {
    val optionType = subsituteAnyInTypeWith(someType, determineGenericType(tpe))
    FastTypeTag(mirror, optionType, optionType.key)
  }

  private def buildFastTypeTagWithInstantiableList(tpe: Type): FastTypeTag[_] = {
    val listType = subsituteAnyInTypeWith(instantiableList, determineGenericType(tpe))
    FastTypeTag(mirror, listType, listType.key)
  }

  private def subsituteAnyInTypeWith(rootType: Type, to: Type): Type =
    rootType.map {
      t => if (t.typeSymbol == anySymbol) to else t
    }

  private def extractToArray[T: ClassTag](readFunction: () => T): Array[T] = {
    val items = new scala.collection.mutable.ArrayBuffer[T]
    val numberOfItems = decoder.readArrayStart()
    (0L until numberOfItems) foreach (_ => items += readFunction())
    items.toArray
  }

  private def determineGenericTypeConvertToTag(t:TypeRef): Some[FastTypeTag[_]] = {
    val gt = determineGenericType(t)
    Some(FastTypeTag(mirror, gt, gt.typeSymbol.fullName))
  }

  private def determineGenericType(tpe: Type): Type =
    tpe match {
      case TypeRef(_, _, genericType :: Nil) => genericType
      case TypeRef(_, _, keyType :: genericType :: Nil) if keyType <:< stringType => genericType // only Map[String, _] supported by avro
      case _ => throw new PicklingException(s"Cannot determine generic type of collection ($tpe)")
    }

  private def isNotRootObject: Boolean = tags.length > 0

  private def parentIsACaseClassOrOption: Boolean = parentIsACaseClass || parentIsAnOption

  private def parentIsACaseClass: Boolean =
    tags.elems match {
      case TypeRef(_, s: ClassSymbol, _) :: tail if s.isCaseClass => true
      case _ => false
    }

  private def parentIsAnOption: Boolean =
    tags.elems match {
      case TypeRef(t, _, _) :: tail if t <:< optionType => true
      case _ => false
    }
}
