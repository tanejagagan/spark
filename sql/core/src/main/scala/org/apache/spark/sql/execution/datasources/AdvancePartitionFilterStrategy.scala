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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._


object AdvancePartitionFilterAdaptor {

  def prunePartitionPredicates(oCatalog: Option[CatalogTable], filters: Seq[Expression],
                               partitionColumns: Seq[Attribute]): Seq[Expression] = {
    val cName = AdvancePartitionFilterStrategy.partitioningClassPropName
    val res = for {
      catalog <- oCatalog.toSeq
      (key, className) <- catalog.properties if key.startsWith(cName)
    } yield {
      Class
      .forName(className)
      .newInstance()
      .asInstanceOf[AdvancePartitionFilterStrategy]
      .prunePartitionPredicates(oCatalog, filters, partitionColumns)
    }
    res.flatten.toSeq
  }

  def prunePartitionPredicates(options : Map[String, String], filters: Seq[Expression],
                               partitionColumns: Seq[Attribute]): Seq[Expression] = {
    val cName = AdvancePartitionFilterStrategy.partitioningClassPropName
    val res = for {
      (key, className) <- options if key.startsWith(cName)
    } yield {
      Class
        .forName(className)
        .newInstance()
        .asInstanceOf[AdvancePartitionFilterStrategy]
        .prunePartitionPredicates(options, filters, partitionColumns)
    }
    res.flatten.toSeq
  }
}

trait TablePropertyBasedStrategy {
  def tableProperties : Map[String, String]
}

object AdvancePartitionFilterStrategy {
  val partitioningClassPropName = "advancePartitionFilterStrategy.class"
}

trait AdvancePartitionFilterStrategy extends TablePropertyBasedStrategy {
  def prunePartitionPredicates( oTable: Option[CatalogTable], filters: Seq[Expression],
      partitionColumns: Seq[Attribute]) : Seq[Expression]

  def prunePartitionPredicates(options: Map[String, String], filters: Seq[Expression],
                                partitionColumns: Seq[Attribute]) : Seq[Expression]

}

