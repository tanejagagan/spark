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

package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.scalatest.WordSpec

import scala.reflect.runtime.universe._

class UnsafeEncoderSpec extends WordSpec {
  val json = """{"intVal": 10, "stringVal": "10", "longVal": 100 }"""
  val schemaStr = "stringVal string, intVal int, longVal bigint"
  val expressions = Array("stringVal",
    "cast(intVal as string) as intVal",
    "cast(longVal as string) as longVal1",
    "(longVal + intVal) as addVal",
    "(longVal - intVal) as subVal",
    "sha2(stringVal, 256) as hash"
  )

  "UnsafeEncoderSpec" should {
    "Value Encoder with builder" in {
      val encoder = UnsafeEncoder.forSchema(schemaStr)
        .build()
      val bytes = encoder.apply(json.getBytes)
      val row = UnsafeRow.readExternal(bytes);
      test(("10", 10, 100L), row)
    }

    "Value Encoder with expression with builder" in {
      val encoder = UnsafeEncoder.forSchema(schemaStr)
        .select(expressions: _*)
        .build()
      val bytes = encoder.apply(json.getBytes)
      val row = UnsafeRow.readExternal(bytes);
      test(("10", "10", "100", 110L, 90L,
        "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5"), row)
    }

    "KeyValue Encoder with no expression null keys" in {
      val keyColumns = Array()
      val json = """{"intVal": 10, "stringVal": "10", "longVal": 100 }"""
      val encoder = UnsafeEncoder.forSchema(schemaStr).forKeyValue
        .build()
      val (kBytes, valBytes) = encoder.apply(json.getBytes)
      val vRow = UnsafeRow.readExternal(valBytes);
      assert(null == kBytes)
      test(("10", 10, 100L), vRow)
    }

    "KeyValue Encoder with no expression" in {
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

    "KeyValue Encoder with no expression null values" in {
      val keyColumns = Array("intVal", "stringVal", "longVal")
      val json = """{"intVal": 10, "stringVal": "10", "longVal": 100 }"""
      val encoder = UnsafeEncoder.forSchema(schemaStr).forKeys(keyColumns)
        .build()
      val (kBytes, valBytes) = encoder.apply(json.getBytes)
      val kRow = UnsafeRow.readExternal(kBytes);
      test((10, "10", 100), kRow)
      assert(null == valBytes)
    }

    "KeyValue Encoder with expressions" in {
      val keyColumns = Array("intVal", "stringVal")
      val encoder = UnsafeEncoder.forSchema(schemaStr)
        .select(expressions: _*)
        .forKeys(keyColumns)
        .build()
      val (kBytes, valBytes) = encoder.apply(json.getBytes)
      val kRow = UnsafeRow.readExternal(kBytes);
      val vRow = UnsafeRow.readExternal(valBytes);
      test(("10", "10"), kRow)
      test(("100", 110, 90, "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5"),
        vRow)
    }
  }

  private def test[T](expected: T, row: UnsafeRow)(implicit tag: TypeTag[T]): Unit = {
    val encoder = ExpressionEncoder[T]()
    val convertBack = encoder.resolveAndBind().fromRow(row)
    assert(expected === convertBack)
  }
}
