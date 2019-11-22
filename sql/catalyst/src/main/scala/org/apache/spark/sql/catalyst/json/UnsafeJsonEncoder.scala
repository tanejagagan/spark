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

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}

import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String


object UnsafeJsonEncoder {

  class Encoder(schemaStr : String,
                jParser : JacksonParser,
                unsafeProjection: UnsafeProjection) extends (Array[Byte] => Array[Byte]) {
    override def apply(bytes: Array[Byte]): Array[Byte] = {
      val rows = jParser.parse[Array[Byte]](bytes, byteArrayParser, b => UTF8String.fromBytes(b) )
      val uRow = unsafeProjection.apply(rows.head)
      UnsafeRow.writeExternal(uRow)
    }

    override def toString(): String = {
      val params = jParser.options.parameters.mkString(",")
      s"""{ "schemaStr" : "$schemaStr", "parserOptions" : "$params" }"""
    }
  }

  val builder : String => Array[Byte] => Array[Byte] = { schemaStr =>
    val schema = StructType.fromDDL(schemaStr)
    val jParser = new JacksonParser(schema, new JSONOptions(Map(), "UTC"))
    val unsafeProjection = UnsafeProjection.create(schema)
    new Encoder(schemaStr, jParser, unsafeProjection)
  }

  def byteArrayParser(jsonFactory: JsonFactory, record: Array[Byte]): JsonParser = {
    jsonFactory.createParser(record, 0, record.length)
  }
}
