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

package org.apache.spark.sql.execution.streaming

import java.util.function.BiFunction

import scala.collection.mutable

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionSpec}
import org.apache.spark.sql.types.StructType



class ConcurrentMetadataLogFileIndex(
                            sparkSession: SparkSession,
                            path: Path,
                            userSpecifiedSchema: Option[StructType],
                            inputMetadataPath : Option[Path] = None,
                            parameters: Map[String, String] = Map.empty )
  extends MetadataLogFileIndex(sparkSession, path, userSpecifiedSchema, inputMetadataPath,
    parameters) {

  private val metadataDirectory = new Path(path, FileStreamSink.metadataDir)
  val map = new java.util.concurrent.ConcurrentHashMap[String, MetadataLogFileIndex]

  private val metadataLog =
    new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, metadataDirectory.toUri.toString)

  val computeFunction = new BiFunction[String, MetadataLogFileIndex, MetadataLogFileIndex]() {
    def apply(key : String, oldValue : MetadataLogFileIndex) : MetadataLogFileIndex = {
      val latestBatchId = metadataLog.getLatest().map(_._1).getOrElse(-1L)
      if (oldValue == null ||
        latestBatchId != oldValue.latestBatchId ) {
         new MetadataLogFileIndex(sparkSession,
          path,
         userSpecifiedSchema,
        inputMetadataPath,
        parameters)
      } else {
        oldValue
      }
    }
  }

  override protected val leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = Map()

  override protected val leafFiles: mutable.LinkedHashMap[Path, FileStatus] =
    new mutable.LinkedHashMap()

  /** Returns the specification of the partitions inferred from the data. */
  override def partitionSpec(): PartitionSpec =
    map.compute(path.toString, computeFunction).partitionSpec()

  override def allFiles(): Seq[FileStatus] =
    map.compute(path.toString, computeFunction).allFiles()

  override def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] =
    map.compute(path.toString, computeFunction).listFiles(partitionFilters, dataFilters)

  override def rootPaths: Seq[Path] = path :: Nil

  /** Refresh any cached file listings */
  override def refresh(): Unit = {}

  def getLatestOffset(path : Path, sparkSession: SparkSession) : Long = {
    val metadataDirectory = new Path(path, FileStreamSink.metadataDir)
    val metadataLog =
      new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession,
        metadataDirectory.toUri.toString)
    metadataLog.getLatest().map(_._1).getOrElse(-1L)
  }
}
