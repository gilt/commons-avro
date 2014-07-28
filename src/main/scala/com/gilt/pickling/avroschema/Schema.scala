package com.gilt.pickling.avroschema


import scala.reflect.runtime.universe._
import com.gilt.pickling.util.Types._
import com.gilt.pickling.util.Types
import java.util.UUID
import org.joda.time.DateTime

object Schema {
  private val namespace = """{"namespace":"""".getBytes
  private val record = """","type":"record"""".getBytes
  private val recordName = ""","name":"""".getBytes

  private val fields = """","fields":[""".getBytes
  private val fieldName = """{"name":"""".getBytes
  private val fieldType = """","type":""".getBytes
  private val endCurlyBracket = "}".getBytes
  private val endSquareBracket = "]".getBytes
  private val commaByte = ','.toByte
  private val comma = Array(commaByte)

  private val arrayBytesField = """"bytes"""".getBytes
  private val arrayFieldStart = """{"type":"array","items":""".getBytes
  private val mapFieldStart = """{"type":"map","values":""".getBytes
  private val optionalFieldStart = """["null",""".getBytes
  private val defaultNull = """, "default": null""".getBytes
  private val uuidKey = "java.util.UUID"
  private val cachedUuidField = """"java.util.UUID"""".getBytes
  private val uuidField = """{"namespace": "java.util", "type": "fixed", "size": 16, "name": "UUID"}""".getBytes
  private val bigDecimalKey = "java.math.BigDecimal"
  private val cachedBigDecimalField = s""""$bigDecimalKey"""".getBytes
  private val bigDecimalField = """{"type": "record","name": "BigDecimal","namespace": "java.math","fields": [{"name": "bigInt", "type": "bytes"},{"name": "scale", "type": "int"}]}""".getBytes
  private val dateTimeKey = "org.joda.time.DateTime"
  private val cachedDateTimeField = s""""$dateTimeKey"""".getBytes
  private val dateTimeField = """{"type": "record","name": "DateTime","namespace": "org.joda.time","fields": [{"name": "timestamp", "type": "long"},{"name": "timezone", "type": "string"}]}""".getBytes

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
  private val dateTimeType = Types.synchronized(typeOf[DateTime])

  private case class Result(schema: Array[Byte], objectCache: Set[String])

  //Synchronized to fix the 2.10 race condition bug in the scala reflective api. 
  def apply[T](implicit ttag: TypeTag[T]): Array[Byte] = Types.synchronized(new Schema(ttag.tpe).bytes)

  def apply[T](clazz: Class[T])(implicit ttag: TypeTag[T]):Array[Byte] = apply[T]

  def apply[T](obj: T)(implicit ttag: TypeTag[T]):Array[Byte] = apply[T]
}

class Schema(caseClassType: Type) {

  import Schema._

  val bytes = processType(caseClassType)

  private def processType(inputTpe: Type) =
    inputTpe match {
      case tpe@TypeRef(_, sym: ClassSymbol, _) if sym.isCaseClass && !(tpe <:< listType) => typeToBytes(caseClassType, Result(Array(), Set())).schema
      case tpe => throw new IllegalArgumentException(s"$tpe is not supported. Only case classes are supported")
    }

  private def typeToBytes(inputTpe: Type, result: Result): Result =
    inputTpe match {
      case tpe if primitiveSymbolToBytes.contains(typeToString(tpe)) =>                               // Primitive Field
        result.copy(schema = result.schema ++ primitiveSymbolToBytes(typeToString(tpe)))
      case tpe if tpe <:< byteArrayType =>                                                            // Bytes Array Field
        result.copy(schema = result.schema ++ arrayBytesField)
      case tpe if tpe <:< uuidType && result.objectCache.contains(uuidKey) =>                         // Cached UUID Field
        result.copy(schema = result.schema ++ cachedUuidField)
      case tpe if tpe <:< uuidType => // UUID Field
        Result(result.schema ++ uuidField, result.objectCache ++ Set(uuidKey))
      case tpe if tpe <:< bigDecimalType && result.objectCache.contains(bigDecimalKey) =>             // Cached BigDecimal Field
        result.copy(schema = result.schema ++ cachedBigDecimalField)
      case tpe if tpe <:< bigDecimalType =>                                                           // BigDecimal Field
        Result(result.schema ++ bigDecimalField, result.objectCache ++ Set(bigDecimalKey))
      case tpe if tpe <:< dateTimeType && result.objectCache.contains(dateTimeKey) =>                 // Cached DateTime Field
        result.copy(schema = result.schema ++ cachedDateTimeField)
      case tpe if tpe <:< dateTimeType =>                                                             // DateTime Field
        Result(result.schema ++ dateTimeField, result.objectCache ++ Set(dateTimeKey))
      case tpe@TypeRef(_, _, keyType :: genericType :: Nil) if supportMapType(tpe, keyType) =>        // Map Field
        val r = typeToBytes(genericType, result.copy(schema = result.schema ++ mapFieldStart))
        r.copy(schema = r.schema ++ endCurlyBracket)
      case tpe@TypeRef(_, _, genericType :: Nil) if supportedIterationType(tpe) =>                    // Iteration Field
        val r = typeToBytes(genericType, result.copy(schema = result.schema ++ arrayFieldStart))
        r.copy(schema = r.schema ++ endCurlyBracket)
      case tpe@TypeRef(_, _, genericType :: Nil) if tpe <:< optionType =>                             // Option Field
        val r = typeToBytes(genericType, result.copy(schema = result.schema ++ optionalFieldStart))
        r.copy(schema = r.schema ++ endSquareBracket ++ defaultNull)
      case tpe if result.objectCache.contains(typeToString(tpe)) =>                                   // Cached case class record
        result.copy(schema = result.schema ++ s""""${typeToString(tpe)}"""".getBytes)
      case tpe@TypeRef(_, s, _) if s.isClass && s.asClass.isCaseClass =>                              // case class field
        val initResult = Result(result.schema ++ recordSchemaPreamble(s), result.objectCache ++ Set(typeToString(tpe)))
        val initObjectResult = tpe.members.filter(!_.isMethod).toList.reverse.foldLeft(initResult) { (r, sym) =>
            val fieldResult = typeToBytes(sym.typeSignature, r.copy(r.schema ++ fieldName ++ sym.name.decoded.trim.getBytes ++ fieldType))
            fieldResult.copy(fieldResult.schema ++ endCurlyBracket ++ comma)
        }
        initObjectResult.copy(schema = removeLastComma(initObjectResult) ++ endSquareBracket ++ endCurlyBracket)
      case tpe => throw new IllegalArgumentException(s"$tpe is not supported. Only case classes are supported")
    }

  private def removeLastComma(result: Result): Array[Byte] =
    if (result.schema.last == commaByte) result.schema.dropRight(1)
    else result.schema

  private def typeToString(tpe: Type) = tpe.typeSymbol.fullName

  private def supportedIterationType(tpe: Type): Boolean = tpe <:< arrayType || tpe <:< setType || tpe <:< seqType

  private def supportMapType(tpe: Type, keyType: Type): Boolean = tpe <:< mapType && keyType <:< stringType

  private def recordSchemaPreamble(typeSymbol: Symbol): Array[Byte] = namespace ++ typeSymbol.owner.fullName.getBytes ++ record ++ recordName ++ typeSymbol.name.decoded.getBytes ++ fields
}

