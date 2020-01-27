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

package org.apache.spark.sql.kafka010

import java.nio.ByteBuffer

import org.scalatest.WordSpec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class KafkaInternalRowSpec extends WordSpec {

  import KafkaInternalRow._

  val partition = 0
  val offset = 1000
  val timestamp = System.currentTimeMillis()
  val key = InternalRow.apply(UTF8String.fromString("hello-key"),
    UTF8String.fromString("world-key"), 123)
  val arrayBackedUnsafeRowKey: UnsafeRow =
    UnsafeProjection.create(Array[DataType](StringType, StringType, IntegerType)).apply(key)
  val value = InternalRow.apply(UTF8String.fromString("hello-value"),
    UTF8String.fromString("world-value"), 456)
  val arrayBackedUnsafeRowValue: UnsafeRow =
    UnsafeProjection.create(Array[DataType](StringType, StringType, IntegerType)).apply(value)

  "KafkaInternalRow" should {

    "With key and value schema" in {
      val partition = 1
      val key = new Key(10)
      val value = new Value(20)
      val kafkaInternalRow = new KafkaInternalRow("topic", partition, key.productArity)
      kafkaInternalRow.pointsTo(10L, 10L, false,
        ByteBuffer.wrap(key.encode), false, ByteBuffer.wrap(value.encode))

      // Test key fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS) == key.keyStr)
      assert(kafkaInternalRow.getInt(FIXED_FIELDS + 1) == key.keyInt)

      // Test value fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS + 2) == value.valStr)
      assert(kafkaInternalRow.getInt(FIXED_FIELDS + 3) == value.valInt)
    }


    "get correct values for various data types for unsafe Row" in {
      val kafkaInternalRow = new KafkaInternalRow("topic", partition, key.numFields)
      val keyArrayBuffer = ByteBuffer.wrap(UnsafeRow.writeExternal(arrayBackedUnsafeRowKey))
      val valueArrayBuffer = ByteBuffer.wrap(UnsafeRow.writeExternal(arrayBackedUnsafeRowValue))
      kafkaInternalRow.pointsTo(offset, timestamp, false, keyArrayBuffer, false, valueArrayBuffer)

      // get fixed Fields
      assert(kafkaInternalRow.getString(TOPIC_FIELD_INDEX) === "topic")
      assert(kafkaInternalRow.getInt(PARTITION_FIELD_INDEX) === partition)
      assert(kafkaInternalRow.getLong(OFFSET_FIELDS_INDEX) === offset)
      assert(kafkaInternalRow.getLong(TIMESTAMP_FIELDS_INDEX) === timestamp)

      // get Key Fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS) === "hello-key")
      assert(kafkaInternalRow.getString(FIXED_FIELDS + 1) === "world-key")
      assert(kafkaInternalRow.getLong(FIXED_FIELDS + 2) === 123)

      // get Value Fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS + 3) === "hello-value")
      assert(kafkaInternalRow.getString(FIXED_FIELDS + 4) === "world-value")
      assert(kafkaInternalRow.getLong(FIXED_FIELDS + 5) === 456)
      assert(kafkaInternalRow.isNullAt(FIXED_FIELDS + 6) === true)
    }

    "get correct value for Kafka Projected Unsafe Row " in {
      val kafkaInternalRow = new KafkaInternalRow("topic", partition, key.numFields)
      val keyArrayBuffer = ByteBuffer.wrap(UnsafeRow.writeExternal(arrayBackedUnsafeRowKey))
      val valueArrayBuffer = ByteBuffer.wrap(UnsafeRow.writeExternal(arrayBackedUnsafeRowValue))
      kafkaInternalRow.pointsTo(offset, timestamp, false, keyArrayBuffer, false, valueArrayBuffer)
      val projectionMap = (0 to 10).reverse.toArray
      val projectRow = new KafkaProjectedUnsafeRow(projectionMap);
      projectRow.pointsTo(kafkaInternalRow)

      // get fixed Fields
      assert(projectRow.getString(FIXED_FIELDS + 6) === "topic")
      assert(projectRow.getInt(FIXED_FIELDS + 5) === partition)
      assert(projectRow.getLong(FIXED_FIELDS + 4) === offset)
      assert(projectRow.getLong(FIXED_FIELDS + 3) === timestamp)

      // get Key Fields
      assert(projectRow.getString(FIXED_FIELDS + 2) === "hello-key")
      assert(projectRow.getString(FIXED_FIELDS + 1) === "world-key")
      assert(projectRow.getLong(FIXED_FIELDS) === 123)

      // get Value Fields
      assert(projectRow.getString(3) === "hello-value")
      assert(projectRow.getString(2) === "world-value")
      assert(projectRow.getLong(1) === 456)
      assert(projectRow.isNullAt(0) === true)
    }


    "get correct value for various data type when key is null" in {
      val partition = 1
      val key = new Key(10)
      val value = new Value(20)
      val kafkaInternalRow = new KafkaInternalRow("topic", partition, key.productArity)
      kafkaInternalRow.pointsTo(10L, 10L, true,
        ByteBuffer.wrap(key.encode), false, ByteBuffer.wrap(value.encode))

      // Test key fields
      // get Key Fields
      // Test key fields
      assert(kafkaInternalRow.isNullAt(FIXED_FIELDS))
      assert(kafkaInternalRow.isNullAt(FIXED_FIELDS + 1))

      // Test value fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS + key.productArity) == value.valStr)
      assert(kafkaInternalRow.getInt(FIXED_FIELDS + key.productArity + 1) == value.valInt)
    }

    "get correct value for various data type when value is null" in {
      val partition = 1
      val key = new Key(10)
      val value = new Value(20)
      val kafkaInternalRow = new KafkaInternalRow("topic", partition, key.productArity)
      kafkaInternalRow.pointsTo(10L, 10L, false,
        ByteBuffer.wrap(key.encode), true, ByteBuffer.wrap(Array[Byte]()))

      // Test key fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS) === "key-10")
      assert(kafkaInternalRow.getInt(FIXED_FIELDS + 1) === 10)

      // Test value fields
      assert(kafkaInternalRow.isNullAt(FIXED_FIELDS + key.productArity))
      assert(kafkaInternalRow.isNullAt(FIXED_FIELDS + key.productArity + 1))
    }

    "get correct value for various data type when value has less fields" in {
      val partition = 1
      val key = new Key(10)
      val value = new Value(20)
      val valueJson = value.toJson
      val kafkaInternalRow = new KafkaInternalRow("topic", partition, key.productArity)
      kafkaInternalRow.pointsTo(10L, 10L, false,
        ByteBuffer.wrap(key.encode), false,
        ByteBuffer.wrap(value.encoderForLessFields(valueJson.getBytes)))

      // Test key fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS) === "key-10")
      assert(kafkaInternalRow.getInt(FIXED_FIELDS + 1) === 10)

      // Test value fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS + key.productArity) === "value-20")
      assert(kafkaInternalRow.isNullAt(FIXED_FIELDS + key.productArity + 1))
    }

    "get correct value for various data type when value schema has more fields" in {
      val partition = 1
      val key = new Key(10)
      val additionalData = "Additional"
      val value = new Value(20, additionalData)
      val kafkaInternalRow = new KafkaInternalRow("topic", partition, key.productArity)
      kafkaInternalRow.pointsTo(10L, 10L, false,
        ByteBuffer.wrap(key.encode), false,
        ByteBuffer.wrap(value.encoderForMoreFields(value.toJson.getBytes)))

      // Test key fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS) === "key-10")
      assert(kafkaInternalRow.getInt(FIXED_FIELDS + 1) === 10)

      // Test value fields
      assert(kafkaInternalRow.getString(FIXED_FIELDS + key.productArity) === "value-20")
      assert(kafkaInternalRow.getInt(FIXED_FIELDS + key.productArity + 1) === 20)
      assert(kafkaInternalRow.getString(FIXED_FIELDS +
        key.productArity + value.productArity - 1) === additionalData)
    }
  }
}
