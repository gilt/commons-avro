package com.gilt.pickling.avro

import scala.language.implicitConversions

package object avro {
  implicit val pickleFormat =
    new AvroPickleFormat

  implicit def toAvroPickle(value: Array[Byte]): AvroPickle =
    AvroPickle(value)
}



