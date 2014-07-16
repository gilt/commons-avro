package com.gilt.pickling

import scala.language.implicitConversions

package object avroschema {
  @Deprecated
  implicit val pickleFormat = new AvroSchemaPickleFormat

  @Deprecated
  implicit def toAvroSchemaPickle(value: Array[Byte]): AvroSchemaPickle = AvroSchemaPickle(value)
}



