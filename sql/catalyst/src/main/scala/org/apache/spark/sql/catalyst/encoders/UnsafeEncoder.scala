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

import scala.collection.mutable

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.json.{ JacksonParser, JSONOptions}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String


object UnsafeEncoder {

  private val functionRegistry = FunctionRegistry.builtin
  private val defaultZoneId = "UTC"

  def forSchema(inputSchema: String): EncoderBuilder = {
    new EncoderBuilder(inputSchema)
  }

  private def byteArrayParser(jsonFactory: JsonFactory, record: Array[Byte]): JsonParser = {
    jsonFactory.createParser(record, 0, record.length)
  }

  private def resolveExpression(e: Expression,
                                attributeMap: Map[String, AttributeReference]): Expression = {
    e.transform {
      case a: UnresolvedAttribute if a.nameParts.size == 1 => attributeMap(a.nameParts.head)
      case fn: UnresolvedFunction => functionRegistry.lookupFunction(fn.name, fn.children)
    }
  }

  // If all Values are null then value should be set to null
  // This will have kafka reclaim during compaction
  private def extractRow(rows: Seq[InternalRow],
                         oProjection: Option[UnsafeProjection],
                         overrideAllNull: Boolean): Array[Byte] = {
    oProjection.map { projection =>
      val rRow = projection.apply(rows.head)
      if (overrideAllNull && allNull(rRow)) {
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

  private def getAttributeName(expr: Expression): String = {
    expr match {
      case Alias(expr, name) => name
      case AttributeReference(name, _, _, _) => name
      case _ => expr.prettyName
    }
  }

  abstract class Builder[T, R](val inputSchema: String) {

    this: T =>

    protected var expressions: Array[String] = _
    protected var options: mutable.Map[String, String] = mutable.Map[String, String]()

    def option(key: String, value: String): T = {
      options.put(key, value)
      return this
    }

    def options(map: Map[String, String]): T = {
      map.foreach(e => options.put(e._1, e._2))
      return this
    }

    def select(col: String*): T = {
      expressions = col.toArray
      return this;
    }

    def format(format: String): T = {
      if (format.toLowerCase != "json") {
        throw new RuntimeException("Only json format is current supported")
      }
      return this;
    }

    def build(): R

  }

  abstract class JsonEncoder[T](inputSchema: String,
                                expressions: Array[String],
                                options: Map[String, String]) {

    protected val schema = StructType.fromDDL(inputSchema)
    protected val jParser = new JacksonParser(schema, new JSONOptions(options, defaultZoneId))
    protected val unresolvedExpressions = expressions.map(CatalystSqlParser.parseExpression(_))
    protected val inputAttributes =
      schema.fields.map(x => AttributeReference(x.name, x.dataType)()).toSeq
    protected val attributeMap =
      inputAttributes.map(a => a.name -> a).toMap
    protected val resolvedAttributes =
      unresolvedExpressions.map(e => resolveExpression(e, attributeMap))
    protected val resolvedAttributeMap = resolvedAttributes.map { a =>
      getAttributeName(a) -> a
    }.toMap

    protected val outputSchemaVal = new StructType(
      resolvedAttributes.map { e =>
        StructField(
          getAttributeName(e), e.dataType)
      }.toArray)
  }

  class EncoderBuilder(inputSchema: String) extends
    Builder[EncoderBuilder, (Array[Byte] => Array[Byte])](inputSchema) {
    def build(): (Array[Byte] => Array[Byte]) = {
      if (expressions != null) {
        return new EncoderWithExpression(inputSchema, expressions, options.toMap)
      } else {
        return new EncoderWithExpression(inputSchema, options.toMap)
      }
    }

    def forKeys(keys: Array[String]): KeyValueEncoderBuilder = {
      val res = new KeyValueEncoderBuilder(inputSchema, keys)
      res.options(options.toMap)
      if (expressions != null) {
        res.select(expressions: _*)
      }
      return res
    }

    def forKeyValue : KeyValueEncoderBuilder = {
      forKeys(Array())
    }
  }

  class KeyValueEncoderBuilder(inputSchema: String,
                               keyColumns: Array[String])
    extends Builder[KeyValueEncoderBuilder,
      (Array[Byte] => (Array[Byte], Array[Byte]))](inputSchema) {

    def build(): (Array[Byte] => (Array[Byte], Array[Byte])) = {
      if (expressions != null) {
        return new KeyValueEncoderWithExpression(inputSchema, keyColumns,
          expressions, options.toMap)
      } else {
        return new KeyValueEncoderWithExpression(inputSchema, keyColumns, options.toMap)
      }
    }
  }

  /*
   * General Behavior. This class is responsible for encoding based on key and values.
   * Key will always be stored the in order provided in keyColumns.
   * Value will always be stored in the order provide in select expression if present or input schema.
   * If all the col inside value is null then value will be set to null in tuple.
   * This assist in Kafka treating those rows to be deleted.
   */
  private final class KeyValueEncoderWithExpression(inputSchema: String,
                                                    keyColumns: Array[String],
                                                    expressions: Array[String],
                                                    options: Map[String, String])
    extends JsonEncoder(inputSchema, expressions, options)
      with (Array[Byte] => (Array[Byte], Array[Byte])) {

    keyColumns.foreach { keyCol =>
      assert(resolvedAttributeMap.contains(keyCol), s"Select Expression " +
        s"${expressions.mkString("'")} do not contain key columns $keyCol")
    }

    private val keyAttributes: Seq[Expression] =
      keyColumns.map(c => resolvedAttributeMap(c)).toSeq
    private val valueSchemaFields = outputSchemaVal.fields
      .filterNot(c => keyColumns.contains(c.name))
    private val valueAttributes: Seq[Expression] =
      valueSchemaFields.map(f => resolvedAttributeMap(f.name))

    private val keyUnsafeProjection = if (keyAttributes.nonEmpty) {
      Some(UnsafeProjection.create(keyAttributes, inputAttributes))
    } else {
      None
    }
    private val valueUnsafeProjection = if (valueAttributes.nonEmpty) {
      Some(UnsafeProjection.create(valueAttributes, inputAttributes))
    } else {
      None
    }

    def this(inputSchema: String,
             keyColumns: Array[String],
             options: Map[String, String]) =
      this(inputSchema, keyColumns, StructType.fromDDL(inputSchema).map(_.name).toArray, options)

    override def apply(bytes: Array[Byte]): (Array[Byte], Array[Byte]) = {
      val rows = jParser.parse[Array[Byte]](bytes, byteArrayParser, b => UTF8String.fromBytes(b))
      val kRow = extractRow(rows, keyUnsafeProjection, false)
      val vRow = extractRow(rows, valueUnsafeProjection, true)
      (kRow, vRow)
    }

    override def toString(): String = {
      val params = jParser.options.parameters.mkString(",")
      val columns = keyColumns.map(c => s"$c").mkString(",")
      s"""{"inputSchema" : "$inputSchema", "keyColumns" : [${columns.mkString(",")}],
         |"expressions": [${expressions.mkString(",")}],
         | "parserOptions" : "$params"}""".stripMargin
    }
  }

  private class EncoderWithExpression(inputSchema: String,
                                      expressions: Array[String],
                                      options: Map[String, String])
    extends JsonEncoder(inputSchema, expressions, options) with (Array[Byte] => Array[Byte]) {

    val valueAttributes: Seq[Expression] =
      outputSchemaVal.fields.map(f => resolvedAttributeMap(f.name))
    val unsafeProjection = UnsafeProjection.create(valueAttributes, inputAttributes)

    def this(inputSchema: String, options: Map[String, String]) =
      this(inputSchema, StructType.fromDDL(inputSchema).map(_.name).toArray, options)

    def outputSchema: StructType = outputSchemaVal

    override def apply(bytes: Array[Byte]): Array[Byte] = {
      val rows = jParser.parse[Array[Byte]](bytes, byteArrayParser, b => UTF8String.fromBytes(b))
      extractRow(rows, Some(unsafeProjection), false)
    }

    override def toString(): String = {
      val params = jParser.options.parameters.mkString(",")
      s"""{ "inputSchema" : "$inputSchema",
         |"expressions": [${expressions.mkString(",")}],
         |"parserOptions" : "$params" }""".stripMargin
    }
  }
}
