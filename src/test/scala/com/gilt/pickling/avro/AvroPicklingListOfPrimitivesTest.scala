package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.avro.TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import scala.collection.JavaConversions._
import java.util.{List => JList}
import AvroPicklingListOfPrimitivesTest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

object AvroPicklingListOfPrimitivesTest {
  case class ListOfInts(list: List[Int])
  case class ListOfLongs(list: List[Long])
  case class ListOfDoubles(list: List[Double])
  case class ListOfFloats(list: List[Float])
  case class ListOfBooleans(list: List[Boolean])
  case class ListOfStrings(list: List[String])
  case class ListOfBytes(list: List[Byte])
  case class ListOfShorts(list: List[Short])
  case class ListOfChars(list: List[Char])
}

class AvroPicklingListOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  // Array of Ints
  test("Pickle a case class with an list of ints") {
    forAll {
      (ints: List[Int]) =>
        val obj = ListOfInts(ints)
        val pckl = obj.pickle
        val depickled = generateBytesFromAvro(obj.list, "/avro/lists/ListOfInts.avsc")
        assert(depickled === pckl.value)
    }
  }

  test("Unpickle a case class with an list of ints") {
    forAll {
      (ints: List[Int]) =>
        val obj = ListOfInts(ints)
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfInts.avsc")
        val hydratedObj: ListOfInts = bytes.unpickle[ListOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of ints") {
    forAll {
      (ints: List[Int]) =>
        val obj = ListOfInts(ints)
        val pckl = obj.pickle
        val hydratedObj: ListOfInts = pckl.unpickle[ListOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Longs
  test("Pickle a case class with an list of longs") {
    forAll {
      (longs: List[Long]) =>
        val obj = ListOfLongs(longs)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of longs") {
    forAll {
      (longs: List[Long]) =>
        val obj = ListOfLongs(longs)
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfLongs.avsc")
        val hydratedObj: ListOfLongs = bytes.unpickle[ListOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of longs") {
    forAll {
      (longs: List[Long]) =>
        val obj = ListOfLongs(longs)
        val pckl = obj.pickle
        val hydratedObj: ListOfLongs = pckl.unpickle[ListOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Doubles
  test("Pickle a case class with an list of doubles") {
    forAll {
      (doubles: List[Double]) =>
        val obj = ListOfDoubles(doubles)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of doubles") {
    forAll {
      (doubles: List[Double]) =>
        val obj = ListOfDoubles(doubles)
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfDoubles.avsc")
        val hydratedObj: ListOfDoubles = bytes.unpickle[ListOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of doubles") {
    forAll {
      (doubles: List[Double]) =>
        val obj = ListOfDoubles(doubles)
        val pckl = obj.pickle
        val hydratedObj: ListOfDoubles = pckl.unpickle[ListOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an list of floats") {
    forAll {
      (floats: List[Float]) =>
        val obj = ListOfFloats(floats)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of floats") {
    forAll {
      (floats: List[Float]) =>
        val obj = ListOfFloats(floats)
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfFloats.avsc")
        val hydratedObj: ListOfFloats = bytes.unpickle[ListOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of floats") {
    forAll {
      (floats: List[Float]) =>
        val obj = ListOfFloats(floats)
        val pckl = obj.pickle
        val hydratedObj: ListOfFloats = pckl.unpickle[ListOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an list of boolean") {
    forAll {
      (booleans: List[Boolean]) =>
        val obj = ListOfBooleans(booleans)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of boolean") {
    forAll {
      (booleans: List[Boolean]) =>
        val obj = ListOfBooleans(booleans)
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfBooleans.avsc")
        val hydratedObj: ListOfBooleans = bytes.unpickle[ListOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of boolean") {
    forAll {
      (booleans: List[Boolean]) =>
        val obj = ListOfBooleans(booleans)
        val pckl = obj.pickle
        val hydratedObj: ListOfBooleans = pckl.unpickle[ListOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Strings
  test("Pickle a case class with an list of string") {
    forAll {
      (strings: List[String]) =>
        val obj = ListOfStrings(strings)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of string") {
    forAll {
      (strings: List[String]) =>
        val obj = ListOfStrings(strings)
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfStrings.avsc")
        val hydratedObj: ListOfStrings = bytes.unpickle[ListOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of string") {
    forAll {
      (strings: List[String]) =>
        val obj = ListOfStrings(strings)
        val pckl = obj.pickle
        val hydratedObj: ListOfStrings = pckl.unpickle[ListOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Bytes
  // TODO A better solution is to write this a as bytebuffer
  test("Pickle a case class with an list of bytes") {
    forAll {
      (bytes: List[Byte]) =>
        val obj = ListOfBytes(bytes)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of bytes") {
    forAll {
      (byteList: List[Byte]) =>
        val obj = ListOfBytes(byteList)
        val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfBytes.avsc")
        val hydratedObj: ListOfBytes = bytes.unpickle[ListOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of bytes") {
    forAll {
      (bytes: List[Byte]) =>
        val obj = ListOfBytes(bytes)
        val pckl = obj.pickle
        val hydratedObj: ListOfBytes = pckl.unpickle[ListOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Shorts
  test("Pickle a case class with an list of shorts") {
    forAll {
      (shorts: List[Short]) =>
        val obj = ListOfShorts(shorts)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of shorts") {
    forAll {
      (shorts: List[Short]) =>
        val obj = ListOfShorts(shorts)
        val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfShorts.avsc")
        val hydratedObj: ListOfShorts = bytes.unpickle[ListOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of shorts") {
    forAll {
      (shorts: List[Short]) =>
        val obj = ListOfShorts(shorts)
        val pckl = obj.pickle
        val hydratedObj: ListOfShorts = pckl.unpickle[ListOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Chars
  test("Pickle a case class with an list of chars") {
    forAll {
      (chars: List[Char]) =>
        val obj = ListOfChars(chars)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of chars") {
    forAll {
      (chars: List[Char]) =>
        val obj = ListOfChars(chars)
        val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfChars.avsc")
        val hydratedObj: ListOfChars = bytes.unpickle[ListOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of chars") {
    forAll {
      (chars: List[Char]) =>
        val obj = ListOfChars(chars)
        val pckl = obj.pickle
        val hydratedObj: ListOfChars = pckl.unpickle[ListOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  private def generateBytesFromAvro(value: JList[Any], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("list", value) // need java collection at this point
    convertToBytes(schema, record)
  }
}
