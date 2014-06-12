package com.gilt.pickling.avroschema

import scala.pickling._
import scala.collection.mutable
import com.gilt.pickling.util.Types
import Types._
import scala.pickling.PicklingException
import scala.Some
import java.util.UUID
import scala.reflect.runtime.universe.typeOf
import com.gilt.pickling.util.Types

object AvroSchemaPickleBuilder {
  private val namespace = """{"namespace":"""".getBytes
  private val record = """","type":"record"""".getBytes
  private val recordName = ""","name":"""".getBytes

  private val fields = """","fields":[""".getBytes
  private val fieldName = """{"name":"""".getBytes
  private val fieldType = """","type":""".getBytes
  private val endCurlyBracket = "}".getBytes
  private val endSquareBracket = "]".getBytes
  private val comma = ",".getBytes

  private val arrayBytesField = """"bytes"""".getBytes
  private val arrayFieldStart = """{"type":"array","items":""".getBytes
  private val mapFieldStart = """{"type":"map","values":""".getBytes
  private val optionalFieldStart = """["null",""".getBytes
  private val uuidKey = "java.util.UUID"
  private val cachedUuidField = """"java.util.UUID"""".getBytes
  private val uuidField = """{"namespace": "java.util", "type": "fixed", "size": 16, "name": "UUID"}""".getBytes
  private val bigDecimalKey = "java.math.BigDecimal"
  private val cachedBigDecimalField = """"java.math.BigDecimal"""".getBytes
  private val bigDecimalField = """{"type": "record","name": "BigDecimal","namespace": "java.math","fields": [{"name": "bigInt", "type": "bytes"},{"name": "scale", "type": "int"}]}""".getBytes

  private val intField = """"int"""".getBytes
  private val stringField = """"string"""".getBytes
  private val primitiveSymbolToBytes = Map(
    KEY_INT -> intField,
    KEY_LONG -> """"long"""".getBytes,
    KEY_FLOAT -> """"float"""".getBytes,
    KEY_DOUBLE -> """"double"""".getBytes,
    KEY_BOOLEAN -> """"boolean"""".getBytes,
    KEY_BYTE -> intField,
    KEY_CHAR -> intField,
    KEY_SHORT -> intField,
    KEY_SCALA_STRING -> stringField,
    KEY_JAVA_STRING -> stringField
  )

  private val mapType = Types.synchronized(typeOf[Map[String, _]])
  private val optionType = Types.synchronized(typeOf[Option[_]])
  private val seqType = Types.synchronized(typeOf[Seq[_]])
  private val setType = Types.synchronized(typeOf[Set[_]])
  private val listType = Types.synchronized(typeOf[List[Any]])
  private val arrayType = Types.synchronized(typeOf[Array[_]])
  private val byteArrayType = Types.synchronized(typeOf[Array[Byte]])
  private val stringType = Types.synchronized(typeOf[String])
  private val uuidType = Types.synchronized(typeOf[UUID])
  private val bigDecimalType = Types.synchronized(typeOf[BigDecimal])
}

//
// This object is synchronized due to scala 2.10 thread safe issue with reflection.
//
// The use of TypeTag is also not ideal and we really should be using FastTypeTag instead. 
//
final class AvroSchemaPickleBuilder(format: AvroSchemaPickleFormat, buffer: AvroSchemaEncodingOutput = new AvroSchemaEncodingOutput()) extends PBuilder with PickleTools {

  import AvroSchemaPickleBuilder._
  import scala.reflect.runtime.universe.{Type, Symbol}

  private var fieldCount = 0
  private val tags = new mutable.Stack[FastTypeTag[_]]()
  private val generatedObjectCache = new mutable.HashSet[String]()

  @inline def beginEntry(picklee: Any): PBuilder = Types.synchronized {
    withHints {
      hints =>
        processObject(hints.tag)
        this
    }
  }

  @inline def putField(name: String, pickler: PBuilder => Unit): PBuilder = Types.synchronized {
    if (fieldCount > 0) buffer.put(comma)
    buffer.put(fieldName ++ name.getBytes ++ fieldType)
    buffer.put(typeToBytes(extractFieldType(name, tags.top)))
    buffer.put(endCurlyBracket)
    fieldCount += 1
    this
  }

  @inline def endEntry(): Unit = Types.synchronized {
    tags.pop()
    if (tags.length == 0)
      buffer.put(endSquareBracket ++ endCurlyBracket)
  }

  @inline def beginCollection(length: Int): PBuilder = this

  @inline def putElement(pickler: PBuilder => Unit): PBuilder = this

  @inline def endCollection(): Unit = {}

  @inline def result() = AvroSchemaPickle(buffer.result())

  private def processObject(tag: FastTypeTag[_]) = {
    import scala.reflect.runtime.universe._
    tag.tpe match {
      case tpe@TypeRef(_, sym: ClassSymbol, _) if sym.isCaseClass && !(tpe <:< listType) =>
        tags.push(tag)
        generatedObjectCache += typeToString(tpe)
        buffer.put(recordSchemaPreamble(sym))
      case _ => throw new PicklingException("Only case classes are supported as root objects")
    }
  }

  private def typeToBytes(inputTpe: Type): Array[Byte] = {
    import scala.reflect.runtime.universe._
    inputTpe match {
      case tpe if primitiveSymbolToBytes.contains(typeToString(tpe)) => primitiveSymbolToBytes(typeToString(tpe)) // Primitive Field
      case tpe if tpe <:< byteArrayType => arrayBytesField // Bytes Array Field
      case tpe if tpe <:< uuidType && generatedObjectCache.contains(uuidKey) => cachedUuidField // Cached UUID Field
      case tpe if tpe <:< uuidType => // UUID Field
        generatedObjectCache += uuidKey
        uuidField
      case tpe if tpe <:< bigDecimalType && generatedObjectCache.contains(bigDecimalKey) => cachedBigDecimalField // Cached BigDecimal Field
      case tpe if tpe <:< bigDecimalType => // BigDecimal Field
        generatedObjectCache += bigDecimalKey
        bigDecimalField
      case tpe@TypeRef(_, _, keyType :: genericType :: Nil) if supportMapType(tpe, keyType) => mapFieldStart ++ typeToBytes(genericType) ++ endCurlyBracket // Map Field
      case tpe@TypeRef(_, _, genericType :: Nil) if supportedIterationType(tpe) => arrayFieldStart ++ typeToBytes(genericType) ++ endCurlyBracket // Iteration Field
      case tpe@TypeRef(_, _, genericType :: Nil) if tpe <:< optionType => optionalFieldStart ++ typeToBytes(genericType) ++ endSquareBracket // Option Field
      case tpe if generatedObjectCache.contains(typeToString(tpe)) => s""""${typeToString(tpe)}"""".getBytes // Cached case class record
      case tpe@TypeRef(_, s, _) if s.isClass && s.asClass.isCaseClass => // case class field
        generatedObjectCache += typeToString(tpe)
        recordSchemaPreamble(s) ++ covertObjectFieldsToSchema(tpe) ++ endSquareBracket ++ endCurlyBracket
      case tpe if tpe <:< FastTypeTag.Unit.tpe || tpe <:< FastTypeTag.Null.tpe => throw new PicklingException("Not supported.")
      case _ => throw new PicklingException("Only case classes are supported")
    }
  }

  private def typeToString(tpe: Type) = tpe.typeSymbol.fullName

  private def supportedIterationType(tpe: Type): Boolean = tpe <:< arrayType || tpe <:< setType || tpe <:< seqType

  private def supportMapType(tpe: Type, keyType: Type): Boolean = tpe <:< mapType && keyType <:< stringType

  private def covertObjectFieldsToSchema(tpe: Type): Array[Byte] = tpe.members.filter(!_.isMethod).map(objectFieldToSchema).reduce(_ ++ comma ++ _)

  private def objectFieldToSchema(sym: Symbol): Array[Byte] = fieldName ++ sym.name.decoded.trim.getBytes ++ fieldType ++ typeToBytes(sym.typeSignature) ++ endCurlyBracket

  private def recordSchemaPreamble(typeSymbol: Symbol): Array[Byte] = namespace ++ typeSymbol.owner.fullName.getBytes ++ record ++ recordName ++ typeSymbol.name.decoded.getBytes ++ fields

  private def extractFieldType(name: String, tag: FastTypeTag[_]): Type =
    tag.tpe.members.filter(!_.isMethod).find(_.name.decoded.trim == name) match {
      case Some(t) => t.typeSignature
      case _ => throw new PicklingException(s"Field $name cannot be found. Should not happen.")
    }

}
