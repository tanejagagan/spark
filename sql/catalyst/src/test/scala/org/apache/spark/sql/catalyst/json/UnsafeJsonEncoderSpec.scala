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

import org.scalatest.WordSpec

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
        val byteArray = encoder(json.getBytes())
        val unsafeRow = UnsafeRow.readExternal(byteArray)
        assert(unsafeRow.getInt(0) == 10)
        assert(unsafeRow.getString(1) == "10")
        assert(unsafeRow.getLong(2) == 100L)
        i += 1
      }
      val end = System.currentTimeMillis()
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
  }
}
