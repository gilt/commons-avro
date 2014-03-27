package com.gilt.pickling.avroschema

import scala.pickling._
import scala.reflect.runtime.universe.{TypeRef, ClassSymbol, Type, Symbol, typeOf}
import scala.collection.mutable
import com.gilt.pickling.util.Types._
import scala.pickling.PicklingException
import scala.Some

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
}

final class AvroSchemaPickleBuilder(format: AvroSchemaPickleFormat, buffer: AvroSchemaEncodingOutput = new AvroSchemaEncodingOutput()) extends PBuilder with PickleTools {

  import AvroSchemaPickleBuilder._
  import com.gilt.pickling.util.Tools._
  import com.gilt.pickling.util.Types._

  private var fieldCount = 0
  private val tags = new mutable.Stack[FastTypeTag[_]]()
  private val generatedObjectCache = new mutable.HashSet[String]()

  @inline def beginEntry(picklee: Any): PBuilder = withHints {
    hints =>
      processObject(hints.tag)
      this
  }

  @inline def putField(name: String, pickler: PBuilder => Unit): PBuilder = {
    if (fieldCount > 0) buffer.put(comma)
    buffer.put(fieldName ++ name.getBytes ++ fieldType)
    buffer.put(typeToBytes(extractFieldType(name, tags.top)))
    buffer.put(endCurlyBracket)
    fieldCount += 1
    this
  }

  @inline def endEntry(): Unit = {
    tags.pop()
    if (tags.length == 0)
      buffer.put(endSquareBracket ++ endCurlyBracket)
  }

  @inline def beginCollection(length: Int): PBuilder = this

  @inline def putElement(pickler: PBuilder => Unit): PBuilder = this

  @inline def endCollection(): Unit = {}

  @inline def result() = AvroSchemaPickle(buffer.result())

  private def processObject(tag: FastTypeTag[_]) =
    tag.tpe match {
      case t@TypeRef(_, s: ClassSymbol, _) if s.isCaseClass && !(t <:< listType) =>
        tags.push(tag)
        generatedObjectCache += t.key
        buffer.put(recordSchemaPreamable(s))
      case _ => throw new PicklingException("Only case classes are supported as root objects")
    }


  private def typeToBytes(tpe: Type): Array[Byte] =
    tpe match {
      case t: TypeRef if primitiveSymbolToBytes.contains(t.key) => primitiveSymbolToBytes(t.key)
      case t: TypeRef if t <:< typeOf[Array[Byte]] => arrayBytesField
      case t@TypeRef(_, _, keyType :: genericType :: Nil) if supportMapType(t, keyType) => mapFieldStart ++ typeToBytes(genericType) ++ endCurlyBracket //Map Field
      case t@TypeRef(_, _, genericType :: Nil) if supportedIterationType(t) => arrayFieldStart ++ typeToBytes(genericType) ++ endCurlyBracket //Iteration Field
      case t@TypeRef(_, _, genericType :: Nil) if t <:< optionType => optionalFieldStart ++ typeToBytes(genericType) ++ endSquareBracket //Option Field
      case t: TypeRef if generatedObjectCache.contains(t.key) => s""""${t.key}"""".getBytes //Cached case class record
      case t@TypeRef(_, s, _) if s.isClass && s.asClass.isCaseClass => // case class field
        generatedObjectCache += t.key
        recordSchemaPreamable(s) ++ covertObjectFieldsToSchema(t) ++ endSquareBracket ++ endCurlyBracket
      case t: TypeRef if t.key == KEY_UNIT || t.key == KEY_NULL => throw new PicklingException("Not supported.")
      case _ => throw new PicklingException("Only case classes are supported")
    }


  private def supportedIterationType(t: TypeRef): Boolean =  t <:< arrayType || t <:< setType || t <:< seqType

  private def supportMapType(t: TypeRef, keyType: Type): Boolean =  t <:< mapType && keyType <:< stringType

  private def covertObjectFieldsToSchema(t: TypeRef): Array[Byte] = t.members.filter(!_.isMethod).map(objectFieldToSchema).reduce(_ ++ comma ++ _)

  private def objectFieldToSchema(sym: Symbol): Array[Byte] = fieldName ++ sym.name.decoded.trim.getBytes ++ fieldType ++ typeToBytes(sym.typeSignature) ++ endCurlyBracket

  private def recordSchemaPreamable(typeSymbol: Symbol): Array[Byte] = namespace ++ typeSymbol.owner.fullName.getBytes ++ record ++ recordName ++ typeSymbol.name.decoded.getBytes ++ fields

  private def extractFieldType(name: String, tag: FastTypeTag[_]): Type =
    tag.tpe.members.filter(!_.isMethod).find(_.name.decoded.trim == name) match {
      case Some(t) => t.typeSignature
      case _ => throw new PicklingException(s"Field $name cannot be found. Should not happen.")
    }

}
