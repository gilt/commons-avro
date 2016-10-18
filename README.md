DEPRECATED
==========

This library has been deprecated. We will not accept pull requests or make any changes to this repo going forward. If you are using gfc-avro already and want to add new features or fix any bugs you may find, please fork the repo and continue development there. 

# Gfc Avro

An Avro library in Scala base on scala pickling. Supports the conversion of case class to avro records or schema
definitions.

## Example Usage
Converting a case class to avro

    case class SomeObject(id: Int)

    import scala.pickling._
    import com.gilt.pickling.avro._

    val obj = SomeObject(1)
    val pckl = obj.pickle

Convert avro bytes to case class

    import scala.pickling._
    import com.gilt.pickling.avro._

    val bytes = ...

    val obj = bytes.unpickle[SomeObject]


Converting a case class to avro schema

    val schema = Schema[SomeObject]


## ToDos/Missing Features
1. No support for Enums
2. No support for fixed length.
3. Schema resolution has not been implemented.
4. List[Byte] and Set[Byte] should be converted avro bytes type instead of an array of bytes.
5. Blocked array or maps are not supported.
6. Writing case classes straight into an 'InputStream'.

## License
Copyright 2014 Gilt Groupe, Inc. 

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0 
