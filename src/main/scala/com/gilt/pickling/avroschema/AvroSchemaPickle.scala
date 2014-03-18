package com.gilt.pickling.avroschema

import scala.pickling.Pickle

case class AvroSchemaPickle(value: Array[Byte]) extends Pickle {
  type ValueType = Array[Byte]
  type PickleFormatType = AvroSchemaPickleFormat

  override def toString = s"""AvroSchemaPickle(${value.mkString("[", ",", "]")})"""
}
