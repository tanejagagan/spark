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

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.util.{Map => JMap}

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String


object UnsafeJsonEncoder {

  private class Encoder(schemaStr : String,
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

  private class KeyValueEncoder(schemaStr : String,
                        keyColumns : Seq[String],
                        jParser: JacksonParser,
                        keyUnsafeProjection: Option[UnsafeProjection],
                        valueUnsafeProjection : Option[UnsafeProjection])
    extends (Array[Byte] => (Array[Byte], Array[Byte])) {

    override def apply(bytes: Array[Byte]): (Array[Byte], Array[Byte]) = {
      val rows = jParser.parse[Array[Byte]](bytes, byteArrayParser, b => UTF8String.fromBytes(b) )
      val kRow = extractRow(rows, keyUnsafeProjection, false)
      val vRow = extractRow(rows, valueUnsafeProjection, true)
      (kRow, vRow)
    }


    // If all Values are null then value should be set to null
    // This will have kafka reclaim during compaction
    private def extractRow(rows: Seq[InternalRow],
                           oProjection: Option[UnsafeProjection],
                           overrideAllNull : Boolean ): Array[Byte] = {
      oProjection.map { projection =>
        val rRow = projection.apply(rows.head)
        if(overrideAllNull && allNull(rRow)) {
          null
        } else {
          UnsafeRow.writeExternal(rRow)
        }
      }.getOrElse(null)
    }

    private def allNull(row: UnsafeRow): Boolean = {
      if (!row.anyNull()) {
        return false
      }
      var i = 0
      val fields = row.numFields()
      while (i < fields) {
        if (!row.isNullAt(i)) {
          return false
        }
        i += 1
      }
      return true
    }

    override def toString(): String = {
      val params = jParser.options.parameters.mkString(",")
      val columns = keyColumns.map(c => s"$c").mkString(",")
      s"""{"schemaStr" : "$schemaStr", "keyColumns" : [${columns.mkString(",")}],
         | "parserOptions" : "$params"}""".stripMargin
    }
  }

  val encoderFor : String => Array[Byte] => Array[Byte] = { schemaStr =>
    val schema = StructType.fromDDL(schemaStr)
    val jParser = new JacksonParser(schema, new JSONOptions(Map(), "UTC"))
    val unsafeProjection = UnsafeProjection.create(schema)
    new Encoder(schemaStr, jParser, unsafeProjection)
  }

  val decoderFor : String =>
    InternalRow => Array[Byte] = { schemaStr =>
    val schema = StructType.fromDDL(schemaStr)
    val fn: InternalRow => Array[Byte] = { row =>
      val stream = new ByteArrayOutputStream()
      val writer =
        new OutputStreamWriter(stream)
      val generator = new JacksonGenerator(schema, writer,
        new JSONOptions(Map(), "UTC"))
      generator.write(row)
      stream.toByteArray
    }
    fn
  }

  val keyValueEncoderFor : (String,
    Array[String]) => Array[Byte] => (Array[Byte], Array[Byte]) = { (schemaStr, keyColumns) =>
    val schema = StructType.fromDDL(schemaStr)
    val jParser = new JacksonParser(schema, new JSONOptions(Map(), "UTC"))
    val inputSchemaAttributes = schema.fields.map(f => new AttributeReference(f.name, f.dataType)())
    val inputSchemaAttributesMap = inputSchemaAttributes.map( a => a.name -> a).toMap
    val valueSchemaFields = schema.fields.filterNot(c => keyColumns.contains(c.name))
    val keyAttributes = keyColumns.map( c => inputSchemaAttributesMap(c))
    val valueAttributes = valueSchemaFields.map( f => inputSchemaAttributesMap(f.name))
    val keyUnsafeProjection = if (keyAttributes.nonEmpty) {
      Some(UnsafeProjection.create(keyAttributes, inputSchemaAttributes))
    } else {
      None
    }
    val valueUnsafeProjection = if (valueAttributes.nonEmpty) {
      Some(UnsafeProjection.create(valueAttributes, inputSchemaAttributes))
    } else {
      None
    }
    new KeyValueEncoder(schemaStr, keyColumns, jParser, keyUnsafeProjection, valueUnsafeProjection)
  }

  def byteArrayParser(jsonFactory: JsonFactory, record: Array[Byte]): JsonParser = {
    jsonFactory.createParser(record, 0, record.length)
  }
}
