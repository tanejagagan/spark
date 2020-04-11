/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.json

import scala.reflect.runtime.universe._
import org.scalatest.WordSpec
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, UnsafeEncoder}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow


class UnsafeJsonEncoderSpec extends WordSpec {
  "EncoderBuilder" should {
    "Encode json correctly" in {
      val schemaStr = "intVal int, stringVal string, longVal bigint"
      val json = """{"intVal": 10, "stringVal": "10", "longVal" : 100 }"""
      val encoder = UnsafeJsonEncoder.encoderFor(schemaStr)
      val encoder2 = UnsafeJsonEncoder.encoderFor(schemaStr + ", floatVal float")
      val start = System.currentTimeMillis()
      var i = 0;
      while (i < 10) {
        // Test Encoder
        val byteArray = encoder(json.getBytes())
        val unsafeRow = UnsafeRow.readExternal(byteArray)
        assert(unsafeRow.getInt(0) === 10)
        assert(unsafeRow.getString(1) === "10")
        assert(unsafeRow.getLong(2) === 100L)

        // Test Decoder
        val decoded = UnsafeJsonEncoder.decoderFor(schemaStr).apply(unsafeRow)
        val str = new String(decoded)
        assert(str.length > 0)
        i += 1
      }
    }

    "Encode json correctly with keys" in {
      val schemaStr = "intVal int, stringVal string, longVal bigint"
      val keyColumns = Array("intVal")
      val json = """{"intVal": 10, "stringVal": "10", "longVal" : 100 }"""
      val encoder = UnsafeJsonEncoder.keyValueEncoderFor(schemaStr, keyColumns)
      val start = System.currentTimeMillis()
      var i = 0;
      while (i < 10) {
        val (keyByteArray, valByteArray) = encoder(json.getBytes())
        val keyUnsafeRow = UnsafeRow.readExternal(keyByteArray)
        val valueUnsafeRow = UnsafeRow.readExternal(valByteArray)
        assert(keyUnsafeRow.getInt(0) == 10)
        assert(valueUnsafeRow.getString(0) == "10")
        assert(valueUnsafeRow.getLong(1) == 100L)
        i += 1
      }
      val end = System.currentTimeMillis()
    }


    "Encode json correctly with keys switched" in {
      val schemaStr = "stringVal string, intVal int, longVal bigint"
      val keyColumns = Array("intVal")
      val json = """{"intVal": 10, "stringVal": "10", "longVal" : 100 }"""
      val encoder = UnsafeJsonEncoder.keyValueEncoderFor(schemaStr, keyColumns)
      val start = System.currentTimeMillis()
      var i = 0;
      while (i < 10) {
        val (keyByteArray, valByteArray) = encoder(json.getBytes())
        val keyUnsafeRow = UnsafeRow.readExternal(keyByteArray)
        val valueUnsafeRow = UnsafeRow.readExternal(valByteArray)
        assert(keyUnsafeRow.getInt(0) == 10)
        assert(valueUnsafeRow.getString(0) == "10")
        assert(valueUnsafeRow.getLong(1) == 100L)
        i += 1
      }
      val end = System.currentTimeMillis()
    }

    "Encode json correctly with no keys " in {
      val schemaStr = "stringVal string, intVal int, longVal bigint"
      val keyColumns = Array[String]()
      val json = """{"intVal": 10, "stringVal": "10", "longVal" : 100 }"""
      val encoder = UnsafeJsonEncoder.keyValueEncoderFor(schemaStr, keyColumns)
      val start = System.currentTimeMillis()
      var i = 0;
      while (i < 10) {
        val (keyByteArray, valByteArray) = encoder(json.getBytes())
        val valueUnsafeRow = UnsafeRow.readExternal(valByteArray)
        assert(keyByteArray == null)
        assert(valueUnsafeRow.getString(0) == "10")
        assert(valueUnsafeRow.getInt(1) == 10)
        assert(valueUnsafeRow.getLong(2) == 100L)
        i += 1
      }
      val end = System.currentTimeMillis()
    }

    "Encode json correctly with keys but null key value " in {
      val schemaStr = "stringVal string, intVal int, longVal bigint"
      val keyColumns = Array[String]("intVal")
      val json = """{"stringVal": "10", "longVal" : 100 }"""
      val encoder = UnsafeJsonEncoder.keyValueEncoderFor(schemaStr, keyColumns)
      val start = System.currentTimeMillis()
      var i = 0;
      while (i < 10) {
        val (keyByteArray, valByteArray) = encoder(json.getBytes())
        val valueUnsafeRow = UnsafeRow.readExternal(valByteArray)
        val keyUnsafeRow = UnsafeRow.readExternal(keyByteArray)
        assert(keyUnsafeRow.isNullAt(0))
        assert(valueUnsafeRow.getString(0) == "10")
        assert(valueUnsafeRow.getLong(1) == 100L)
        i += 1
      }
      val end = System.currentTimeMillis()
    }


    "Encode json correctly with null values " in {
      val schemaStr = "stringVal string, intVal int, longVal bigint"
      val keyColumns = Array("intVal", "stringVal")
      val json = """{"intVal": 10, "stringVal": "10" }"""
      val encoder = UnsafeJsonEncoder.keyValueEncoderFor(schemaStr, keyColumns)
      val start = System.currentTimeMillis()
      var i = 0;
      while (i < 10) {
        val (keyByteArray, valByteArray) = encoder(json.getBytes())
        val keyUnsafeRow = UnsafeRow.readExternal(keyByteArray)
        assert(valByteArray === null)
        assert(keyUnsafeRow.getString(1) == "10")
        assert(keyUnsafeRow.getInt(0) == 10)
        i += 1
      }
      val end = System.currentTimeMillis()
    }

    /*
    "Encode with expression" in {
      val schemaStr = "stringVal string, intVal int, longVal bigint"
      val keyColumns = Array("intVal", "stringVal")
      val json = """{"intVal": 10, "stringVal": "10" }"""
      val expressions = Array("stringVal", "cast(intVal as string)")
      val expressionEncoder = UnsafeJsonEncoder.encoderForSchemaAndExpression(schemaStr,
        expressions)
      val bytes = expressionEncoder.apply(json.getBytes)
      val uRow = UnsafeRow.readExternal(bytes);
      assert( uRow.getString(0) == "10")
      assert( uRow.getString(1) == "10")
    }

    "KeyValue Encode with expression" in {
      val schemaStr = "stringVal string, intVal int, longVal bigint"
      val keyColumns = Array("intVal", "stringVal")
      val json = """{"intVal": 10, "stringVal": "10", "longVal": 100 }"""
      val expressions = Array("stringVal", "cast(intVal as string) as intVal",
        "cast(longVal as string) as longVal1",
        "(longVal + intVal) as addVal",
        "(longVal - intVal) as subVal",
        "sha2(stringVal, 256) as hash"
        )
      val expressionEncoder = UnsafeJsonEncoder.keyValueEncoderForSchemaAndExpression(
        schemaStr, keyColumns, expressions
      )
      val (kBytes, valBytes) = expressionEncoder.apply(json.getBytes)
      val kRow = UnsafeRow.readExternal(kBytes);
      val vRow = UnsafeRow.readExternal(valBytes);
      assert( kRow.getString(0) == "10")
      assert( kRow.getString(1) == "10")
      assert("100" == vRow.getString(0))
      assert(110 == vRow.getLong(1))
      assert(90 === vRow.getLong(2))
      assert("4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5" ==
        vRow.getString(3))
    }

     */

    "KeyValue Encode with expression with builder" in {
      val schemaStr = "stringVal string, intVal int, longVal bigint"
      val keyColumns = Array("intVal", "stringVal")
      val json = """{"intVal": 10, "stringVal": "10", "longVal": 100 }"""
      val expressions = Array("stringVal", "cast(intVal as string) as intVal",
        "cast(longVal as string) as longVal1",
        "(longVal + intVal) as addVal",
        "(longVal - intVal) as subVal",
        "sha2(stringVal, 256) as hash"
      )
      val builder = UnsafeJsonEncoder.keyValueEncoderBuilder(schemaStr)
      val encoder = builder.keyColumns(keyColumns: _*)
        .select(expressions : _*)
        .build()
      val (kBytes, valBytes) = encoder.apply(json.getBytes)
      val kRow = UnsafeRow.readExternal(kBytes);
      val vRow = UnsafeRow.readExternal(valBytes);
      test(("10", "10"), kRow)
      test(("100", 110, 90, "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5"),
        vRow)
    }
  }

  "KeyValue Encoder with expression with builder" in {
    val schemaStr = "stringVal string, intVal int, longVal bigint"
    val keyColumns = Array("intVal", "stringVal")
    val json = """{"intVal": 10, "stringVal": "10", "longVal": 100 }"""
    val expressions = Array("stringVal",
      "cast(intVal as string) as intVal",
      "cast(longVal as string) as longVal1",
      "(longVal + intVal) as addVal",
      "(longVal - intVal) as subVal",
      "sha2(stringVal, 256) as hash"
    )
    val encoder = UnsafeEncoder.forSchema(schemaStr)
      .select(expressions : _*)
      .forKeys(keyColumns)
      .build()
    val (kBytes, valBytes) = encoder.apply(json.getBytes)
    val kRow = UnsafeRow.readExternal(kBytes);
    val vRow = UnsafeRow.readExternal(valBytes);
    test(("10", "10"), kRow)
    test(("100", 110, 90, "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5"), vRow)
  }

  "KeyValue Encoder with no expression" in {
    val schemaStr = "stringVal string, intVal int, longVal bigint"
    val keyColumns = Array("intVal", "stringVal")
    val json = """{"intVal": 10, "stringVal": "10", "longVal": 100 }"""
    val encoder = UnsafeEncoder.forSchema(schemaStr).forKeys(keyColumns)
      .build()
    val (kBytes, valBytes) = encoder.apply(json.getBytes)
    val kRow = UnsafeRow.readExternal(kBytes);
    val vRow = UnsafeRow.readExternal(valBytes);
    test((10, "10"), kRow)
    test((100L), vRow)
  }

  "Value Encoder with expression with builder" in {
    val schemaStr = "stringVal string, intVal int, longVal bigint"
    // val keyColumns = Array("intVal", "stringVal")
    val json = """{"intVal": 10, "stringVal": "10", "longVal": 100 }"""
    val expressions = Array("stringVal",
      "cast(intVal as string) as intValStr",
      "cast(longVal as string) as longVal1",
      "(longVal + cast(intVal as bigint)) as addVal",
      "(longVal - cast(intVal as bigint)) as subVal",
      "sha2(stringVal, 256) as hash"
    )
    val encoder = UnsafeEncoder.forSchema(schemaStr)
      .select(expressions : _*)
      .build()
    val bytes = encoder.apply(json.getBytes)
    val row = UnsafeRow.readExternal(bytes);
    test(("10", "10", "100", 110L, 90L,
      "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5"), row)
  }

  "Value Encoder with  with builder" in {
    val schemaStr = "stringVal string, intVal int, longVal bigint"
    // val keyColumns = Array("intVal", "stringVal")
    val json = """{"intVal": 10, "stringVal": "10", "longVal": 100 }"""

    val encoder = UnsafeEncoder.forSchema(schemaStr)
      .build()
    val bytes = encoder.apply(json.getBytes)
    val row = UnsafeRow.readExternal(bytes);
    test(("10", 10, 100L), row)
  }

  private def test[T]( expected : T, row : UnsafeRow)(implicit tag: TypeTag[T]): Unit = {
    val encoder = ExpressionEncoder[T]()
    val convertBack = encoder.resolveAndBind().fromRow(row)
    assert(expected === convertBack)
  }
}

