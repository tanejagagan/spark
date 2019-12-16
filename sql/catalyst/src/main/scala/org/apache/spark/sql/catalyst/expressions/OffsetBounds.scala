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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, expr3, expr4) - If `expr1` evaluates to true, then returns `expr2`; otherwise returns `expr3`.",
  examples = """
    Examples:
      > SELECT _FUNC_(parttion, offset, "0:100, 2:200", "0:150, 1:210" );
       a
  """)
// scalastyle:on line.size.limit
case class OffsetBounds(partition: Expression, offset : Expression,
                        lowerBound: Expression, upperBound: Expression)
  extends Expression {

  override def children: Seq[Expression] = partition :: offset :: lowerBound :: upperBound :: Nil
  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (partition.dataType != IntegerType) {
      TypeCheckResult.TypeCheckFailure(
        "type of predicate expression in OffsetBounds should be Partition column, " +
          s"not ${partition.dataType.catalogString}")
    } else if (offset.dataType != LongType) {
      TypeCheckResult.TypeCheckFailure(
        "type of predicate expression in OffsetBounds should be Partition column, " +
          s"not ${offset.dataType.catalogString}")
    } else {
        TypeCheckResult.TypeCheckSuccess
    }
  }

  override def eval(input: InternalRow): Any = {
    java.lang.Boolean.TRUE
  }

  def getLowerBounds : Option[Map[Int, Long]] = {
    if (lowerBound == null) {
      None
    } else {
      Some(OffsetBounds.parseBoundString(lowerBound.eval(null).toString))
    }
  }

  def getUpperBounds : Option[Map[Int, Long]] = {
    if (upperBound == null) {
      None
    } else {
      Some(OffsetBounds.parseBoundString(upperBound.eval(null).toString))
    }
  }

  override  def dataType: DataType = BooleanType

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val code =
       code"""
            |boolean ${ev.isNull} = false;
            |boolean ${ev.value} = true;
       """.stripMargin
    ev.copy(code = code)
  }

  override def toString: String = s"OffsetBounds ($partition, $offset, $upperBound, $lowerBound)"

  override def sql: String = s"offset_bounds (${partition.sql}, ${offset.sql}, " +
    "${upperBound.sql}, ${lowerBound.sql})"
}

object OffsetBounds {
  def parseBoundString(str  : String ): Map[Int, Long] = {
    str.split(",").map{ i =>
      val splits = i.split(":")
      if(splits.size !=2) {
        throw new RuntimeException(
          """Incorrect format. Please use format "0:100, 2:200" to specify start and end offset""")
      }
      splits(0).trim.toInt -> splits(1).trim.toLong
    }.toMap
  }
}
