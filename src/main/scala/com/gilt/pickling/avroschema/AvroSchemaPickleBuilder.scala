package com.gilt.pickling.avroschema

import scala.pickling._
import scala.reflect.runtime.universe.TypeRef
import scala.reflect.runtime.{universe => ru}
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
  private val optionalFieldStart = """["null",""".getBytes

  private val optionType = ru.typeOf[Option[_]]
  private val arrayType = ru.typeOf[Array[_]]
  private val iterableType = ru.typeOf[Iterable[_]]

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

  private val tags = new mutable.Stack[FastTypeTag[_]]()
  private var fieldCount = 0

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

  private def processObject(tag: FastTypeTag[_]) = {
    tag.tpe match {
      case TypeRef(_, s, _) if s.isClass && s.asClass.isCaseClass =>
        tags.push(tag)
        buffer.put(recordSchemaPreamable(s))
      case _ => throw new PicklingException("Only case classes are supported as root objects")
    }
  }

  private def typeToBytes(tpe: ru.Type): Array[Byte] = {
    import com.gilt.pickling.util.Tools._
    tpe match {
      case t: TypeRef if primitiveSymbolToBytes.contains(t.key) => primitiveSymbolToBytes(t.key)
      case t: TypeRef if t <:< ru.typeOf[Array[Byte]] => arrayBytesField
      case t@TypeRef(_, _, genericType :: Nil) if t <:< arrayType || t <:< iterableType => arrayFieldStart ++ typeToBytes(genericType) ++ endCurlyBracket
      case t@TypeRef(_, _, genericType :: Nil) if t <:< optionType => optionalFieldStart ++ typeToBytes(genericType) ++ endSquareBracket
      case t@TypeRef(_, s, _) if s.isClass && s.asClass.isCaseClass =>
        recordSchemaPreamable(s) ++ covertObjectFieldToSchema(t) ++ endSquareBracket ++ endCurlyBracket
      case t: TypeRef if t.key == KEY_UNIT || t.key == KEY_NULL => throw new PicklingException("Not supported.")
      case _ => throw new PicklingException("Only case classes are supported")
    }
  }

  private def covertObjectFieldToSchema(t: ru.TypeRef): Array[Byte] =
    t.members.filter(!_.isMethod).map(objectFieldToSchema).reduce(_ ++ comma ++ _)

  private def objectFieldToSchema(sym: ru.Symbol): Array[Byte] =
    fieldName ++ sym.name.decoded.trim.getBytes ++ fieldType ++ typeToBytes(sym.typeSignature) ++ endCurlyBracket

  private def recordSchemaPreamable(typeSymbol: ru.Symbol): Array[Byte] =
    namespace ++ typeSymbol.owner.fullName.getBytes ++ record ++ recordName ++ typeSymbol.name.decoded.getBytes ++ fields

  private def extractFieldType(name: String, tag: FastTypeTag[_]): ru.Type =
    tag.tpe.members.filter(!_.isMethod).find(_.name.decoded.trim == name) match {
      case Some(t) => t.typeSignature
      case _ => throw new PicklingException(s"Field $name cannot be found. Should not happen.")
    }

}
