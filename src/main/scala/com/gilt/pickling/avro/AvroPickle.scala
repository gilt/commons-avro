package com.gilt.pickling.avro

import scala.pickling.Pickle

case class AvroPickle(value: Array[Byte]) extends Pickle {
  type ValueType = Array[Byte]
  type PickleFormatType = AvroPickleFormat

  override def toString = s"""AvroPickle(${value.mkString("[", ",", "]")})"""
}
