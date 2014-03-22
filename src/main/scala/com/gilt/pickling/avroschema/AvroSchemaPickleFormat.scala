package com.gilt.pickling.avroschema

import scala.pickling.{PBuilder, FastTypeTag, PickleFormat}
import scala.reflect.runtime.universe.Mirror

class AvroSchemaPickleFormat extends PickleFormat {

  type PickleType = AvroSchemaPickle
  type OutputType = AvroSchemaEncodingOutput

  def createBuilder() =
      new AvroSchemaPickleBuilder(this)
//    new Blah(this)

  def createBuilder(out: AvroSchemaEncodingOutput): PBuilder =
      new AvroSchemaPickleBuilder(this, out)
//    new Blah(this, out)

  def createReader(pickle: PickleType, mirror: Mirror) =
    throw new UnsupportedOperationException("Avro schema unpickler is not supported.")

}
