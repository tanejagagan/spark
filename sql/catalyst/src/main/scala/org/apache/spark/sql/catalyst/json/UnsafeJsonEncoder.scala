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

import scala.collection.mutable

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, InterpretedProjection, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String




object UnsafeJsonEncoder {

  val functionRegistry = FunctionRegistry.builtin


  def encoderFor(inputSchema : String): (Array[Byte] => Array[Byte]) = {
    return new Encoder(inputSchema);
  }



  def encoderBuilder(inputSchema : String) : EncoderBuilder = {
    return new EncoderBuilder(inputSchema)
  }

  def keyValueEncoderBuilder(inputSchema : String) : KeyValueEncoderBuilder = {
    return new KeyValueEncoderBuilder(inputSchema)
  }

  abstract class Builder[T, R] (val inputSchema : String) {

    this: T =>

    protected var expressions : Array[String] = _
    protected var options : mutable.Map[String, String] = mutable.Map[String, String]()

    def option(key : String, value : String): T = {
      options.put(key, value)
      return this
    }

    def options(map : Map[String, String]) : T = {
      map.foreach( e => options.put(e._1, e._2))
      return this
    }

    def select(col : String*): T = {
      expressions = col.toArray
      return this;
    }

    def build(): R

  }

  class EncoderBuilder( inputSchema : String)
    extends Builder[EncoderBuilder, (Array[Byte] => Array[Byte])](inputSchema) {
    def build(): (Array[Byte] => Array[Byte]) = {
      if (expressions != null) {
        return new EncoderWithExpression(inputSchema, expressions)
      } else {
        return new Encoder(inputSchema)
      }
    }
  }

  class KeyValueEncoderBuilder(inputSchema : String)
    extends Builder[KeyValueEncoderBuilder,
      (Array[Byte] => (Array[Byte], Array[Byte]))](inputSchema) {
    protected var keyColumns : Array[String] = _

     def keyColumns(col : String*) : KeyValueEncoderBuilder = {
      keyColumns = col.toArray
      return this
    }

    def build(): (Array[Byte] => (Array[Byte], Array[Byte])) = {
      if (expressions != null) {
        return new KeyValueEncoderWithExpression(inputSchema, keyColumns, expressions)
      } else {
        return new KeyValueEncoder(inputSchema, keyColumns)
      }
    }
  }

  /*
  class EncoderBuilder(val inputSchema : String) {

    protected var expressions : Array[String] =_
    protected var options : mutable.Map[String, String]  = mutable.Map[String, String]()

    def option(key : String, value : String): EncoderBuilder = {
      options.put(key, value)
      return this
    }

    def options(map : Map[String, String]) : EncoderBuilder = {
      map.foreach( e => options.put(e._1, e._2))
      return this
    }

    def select(col : String*): EncoderBuilder ={
      expressions = col.toArray
      return this;
    }

    def build(): (Array[Byte] => Array[Byte]) = {
      if (expressions != null) {
        return new EncoderWithExpression(inputSchema, expressions)
      } else {
        return new Encoder(inputSchema)
      }
    }
  }

  class KeyValueEncoderBuilder(val inputSchema : String) {

    protected var expressions : Array[String] =_
    protected var keyColumns : Array[String] =_
    protected var options : mutable.Map[String, String]  = mutable.Map[String, String]()

    def option(key : String, value : String): KeyValueEncoderBuilder  = {
      options.put(key, value)
      return this
    }

    def options(map : Map[String, String]) : KeyValueEncoderBuilder = {
      map.foreach( e => options.put(e._1, e._2))
      return this
    }

    def select(col : String*): KeyValueEncoderBuilder ={
      expressions = col.toArray
      return this;
    }

    def keyColumns(col : String*) : KeyValueEncoderBuilder = {
      keyColumns = col.toArray
      return this
    }

    def build(): (Array[Byte] => (Array[Byte], Array[Byte])) = {
      if (expressions != null) {
        return new KeyValueEncoderWithExpression(inputSchema, keyColumns, expressions)
      } else {
        return new KeyValueEncoder(inputSchema, keyColumns)
      }
    }
  }

   */


  def encoderFor(inputSchema : String,
                            expressions : Array[String]) : (Array[Byte] => Array[Byte]) = {
    new EncoderWithExpression(inputSchema, expressions)
  }

  def keyValueEncoderFor(inputSchema : String,
                         keyColumns : Array[String] ) : Array[Byte] =>
    (Array[Byte], Array[Byte]) = {
    return new KeyValueEncoder(inputSchema, keyColumns)
  }

  def keyValueEncoderFor(inputSchema : String,
                  keyColumns : Array[String],
                  expressions : Array[String]) : (Array[Byte] => (Array[Byte], Array[Byte])) = {
    new KeyValueEncoderWithExpression(inputSchema, keyColumns, expressions)
  }

  def decoderFor( schemaStr : String ) : InternalRow => Array[Byte] = {
    val schema = StructType.fromDDL(schemaStr)
    val fn: InternalRow => Array[Byte] = { row =>
      val byteBufferStream = new ByteArrayOutputStream()
      val outputStreamWriter = new OutputStreamWriter(byteBufferStream);
      val generator = new JacksonGenerator(schema, outputStreamWriter,
        new JSONOptions(Map(), "UTC"))
      generator.write(row)
      generator.flush()
      byteBufferStream.toByteArray
    }
    fn
  }



  private class KeyValueEncoderWithExpression(inputSchema : String,
                                              keyColumns : Array[String],
                                              expressions : Array[String] )
    extends (Array[Byte] => (Array[Byte], Array[Byte])) {
    val unresolvedExpressions = expressions.map(CatalystSqlParser.parseExpression(_))
    val schema = StructType.fromDDL(inputSchema)
    val inputAttributes = schema.fields.map(x => AttributeReference(x.name, x.dataType)()).toSeq
    val attributeMap = inputAttributes.map(a => a.name -> a).toMap
    val resolvedAttributes = unresolvedExpressions.map(e => resolveExpression(e, attributeMap))
    val resolvedAttributeMap = resolvedAttributes.map{ a => getAttributeName(a)->a}.toMap


    val jParser = new JacksonParser(schema, new JSONOptions(Map(), "UTC"))
    val projection = new InterpretedProjection(resolvedAttributes, inputAttributes)

    val outputSchemaVal = new StructType(
      resolvedAttributes.map { e => StructField(
        getAttributeName(e), e.dataType) }.toArray)
    // val unsafeProjection = UnsafeProjection.create(outputSchemaVal)
    val outputSchemaMap = outputSchemaVal.fields.map( f => f.name -> f).toMap


    val valueSchemaFields = outputSchemaVal.fields.filterNot(c => keyColumns.contains(c.name))
    val keyAttributes : Seq[Expression] =
      keyColumns.map( c => resolvedAttributeMap(c)).toSeq
    val valueAttributes : Seq[Expression] =
      valueSchemaFields.map( f => resolvedAttributeMap(f.name))

    val keyUnsafeProjection = if (keyAttributes.nonEmpty) {
      Some(UnsafeProjection.create(keyAttributes, inputAttributes))
    } else {
      None
    }
    val valueUnsafeProjection = if (valueAttributes.nonEmpty) {
      Some(UnsafeProjection.create(valueAttributes, inputAttributes))
    } else {
      None
    }


    override def apply(bytes: Array[Byte]): (Array[Byte], Array[Byte]) = {
      val rows = jParser.parse[Array[Byte]](bytes, byteArrayParser, b => UTF8String.fromBytes(b) )
      val kRow = extractRow(rows, keyUnsafeProjection, false)
      val vRow = extractRow(rows, valueUnsafeProjection, true)
      (kRow, vRow)
    }

    def getAttributeName(expr : Expression ): String = {
      expr match {
        case Alias(expr, name) => name
        case AttributeReference(name, _, _, _) => name
        case  _ => expr.prettyName
      }
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
      s"""{"schemaStr" : "$inputSchema", "keyColumns" : [${columns.mkString(",")}],
         | "parserOptions" : "$params"}""".stripMargin
    }
  }




  private class EncoderWithExpression( inputSchema : String,
                                expressions : Array[String])
         extends (Array[Byte] => Array[Byte]) {
     val unresolvedExpressions = expressions.map(CatalystSqlParser.parseExpression(_))
     val schema = StructType.fromDDL(inputSchema)
     val inputAttributes = schema.fields.map(x => AttributeReference(x.name, x.dataType)()).toSeq
     val attributeMap = inputAttributes.map(a => a.name -> a).toMap
     val resolvedAttributes = unresolvedExpressions.map(e => resolveExpression(e, attributeMap))
     val jParser = new JacksonParser(schema, new JSONOptions(Map(), "UTC"))
     val projection = new InterpretedProjection(resolvedAttributes, inputAttributes)

     val outputSchemaVal = new StructType(
       resolvedAttributes.map { e => StructField(e.prettyName, e.dataType) }.toArray)

     def outputSchema: StructType = outputSchemaVal

     val unsafeProjection = UnsafeProjection.create(outputSchemaVal)

     override def apply(bytes: Array[Byte]): Array[Byte] = {
       val rows = jParser.parse[Array[Byte]](bytes, byteArrayParser, b => UTF8String.fromBytes(b))
       val projectedRow = projection.apply(rows.head)
       val uRow = unsafeProjection.apply(projectedRow)
       UnsafeRow.writeExternal(uRow)
     }
  }

  private class Encoder(schemaStr : String
                // jParser : JacksonParser,
                // unsafeProjection: UnsafeProjection
                       ) extends (Array[Byte] => Array[Byte]) {

    val schema = StructType.fromDDL(schemaStr)
    val jParser = new JacksonParser(schema, new JSONOptions(Map(), "UTC"))
    val unsafeProjection = UnsafeProjection.create(schema)

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
                        keyColumns : Seq[String]
                               )
                        // jParser: JacksonParser,
                        // keyUnsafeProjection: Option[UnsafeProjection],
                        // valueUnsafeProjection : Option[UnsafeProjection])
    extends (Array[Byte] => (Array[Byte], Array[Byte])) {

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

  /*

  val encoderFor : String => Array[Byte] => Array[Byte] = { schemaStr =>
    // val schema = StructType.fromDDL(schemaStr)
    // val jParser = new JacksonParser(schema, new JSONOptions(Map(), "UTC"))
    // val unsafeProjection = UnsafeProjection.create(schema)
    new Encoder(schemaStr) // , jParser, unsafeProjection)
  }

  val decoderFor : String =>
    InternalRow => Array[Byte] = { schemaStr =>
    val schema = StructType.fromDDL(schemaStr)
    val fn: InternalRow => Array[Byte] = { row =>
      val byteBufferStream = new ByteArrayOutputStream()
      val outputStreamWriter = new OutputStreamWriter(byteBufferStream);
      val generator = new JacksonGenerator(schema, outputStreamWriter,
        new JSONOptions(Map(), "UTC"))
      generator.write(row)
      generator.flush()
      byteBufferStream.toByteArray
    }
    fn
  }

  val keyValueEncoderFor : (String,
    Array[String]) => Array[Byte] => (Array[Byte], Array[Byte]) = { (schemaStr, keyColumns) =>
    /*
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

     */
    new KeyValueEncoder(schemaStr, keyColumns)
  }

   */

  def byteArrayParser(jsonFactory: JsonFactory, record: Array[Byte]): JsonParser = {
    jsonFactory.createParser(record, 0, record.length)
  }

  private def resolveExpression(e: Expression,
                                attributeMap: Map[String, AttributeReference]): Expression =
    e.transform {
      case a: UnresolvedAttribute if a.nameParts.size == 1 => attributeMap(a.nameParts.head)
      case fn : UnresolvedFunction => functionRegistry.lookupFunction(fn.name, fn.children )
    }
}
