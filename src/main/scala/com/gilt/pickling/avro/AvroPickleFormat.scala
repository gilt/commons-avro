package com.gilt.pickling.avro

import scala.pickling.{PBuilder, FastTypeTag, PickleFormat}
import scala.reflect.runtime.universe.Mirror

class AvroPickleFormat extends PickleFormat {
  type PickleType = AvroPickle
  type OutputType = AvroEncodingOutput

  def createBuilder() =
    new AvroPickleBuilder(this, null)

  def createBuilder(out: AvroEncodingOutput): PBuilder =
    new AvroPickleBuilder(this, out)

  def createReader(pickle: PickleType, mirror: Mirror) =
    new AvroPickleReader(pickle.value, mirror, this)
}
