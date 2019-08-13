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

import scala.collection.mutable

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class NGMetadataLogFileIndex(sparkSession: SparkSession,
                             path: Path,
                             val userSpecifiedPartitionSchema: Option[StructType],
                             inputMetadataPath: Option[Path] = None,
                             val parameters: Map[String, String] = Map.empty)
  extends FileIndex with Logging {

  private val metadataDirectory = new Path(path, FileStreamSink.metadataDir)
  logInfo(s"Reading streaming file log from $metadataDirectory")

  private val metadataLog =
    new NGFileStreamSinkLog(FileStreamSinkLog.VERSION,
      sparkSession, metadataDirectory.toUri.toString,
      path.toUri.toString, userSpecifiedPartitionSchema)

  override def rootPaths: Seq[Path] = path :: Nil

  override def refresh(): Unit = {}

  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val partitionColumnRefs: Seq[Attribute] = partitionSchema.fields.map { field =>
      val ar: AttributeReference = AttributeReference(field.name, field.dataType)()
      ar
    }

    val inferredFilters = AdvancePartitionFilterAdaptor.prunePartitionPredicates(parameters,
      dataFilters, partitionColumnRefs)
    metadataLog.listFiles(partitionFilters ++ inferredFilters)
  }

  override def inputFiles: Array[String] = {
    metadataLog.inputFiles
  }

  /** Sum of table file sizes, in bytes */
  override def sizeInBytes: Long = {
    metadataLog.sizeInBytes
  }

  def allFiles(): Seq[FileStatus] = {
    metadataLog.allFiles()
  }

  /** Schema of the partitioning columns, or the empty schema if the table is not partitioned. */
  override def partitionSchema: StructType = metadataLog.getPartitionSchema()
}

case class PartitionAndStatus(partition: Any, files: Seq[SinkFileStatus])

class NGFileStreamSinkLog(metadataLogVersion: Int,
                          sparkSession: SparkSession,
                          path: String,
                          baseDataPath: String,
                          userSpecifiedPartitionSchema: Option[StructType],
                          format: String = "parquet",
                          val parameters: Map[String, String] = Map.empty) extends
  HDFSMetadataLog[Array[SinkFileStatus]](sparkSession, path) {

  val batchIdColName = "batchId"

  val sinkStatusFields = Seq(StructField(batchIdColName, LongType, false),
    StructField("path", StringType, false),
    StructField("size", LongType, false),
    StructField("isDir", BooleanType, false),
    StructField("modificationTime", LongType, false),
    StructField("blockReplication", IntegerType, false),
    StructField("blockSize", LongType, false),
    StructField("action", StringType, false))

  val sinkStatusStructStr = s"collect_list(struct(${sinkStatusFields.map(_.name).mkString(",")}))"

  private var currentBatchAndDf: (Option[Long], DataFrame) = {
    val latestBatchId = getLatestBatchId()
    if (latestBatchId.isDefined) {
      (latestBatchId, readDF(latestBatchId.get))
    } else {
      assert(userSpecifiedPartitionSchema.isDefined,
        "Cannot infer partition schema")
      val emptyDataFrame =
        sparkSession.createDataFrame(
          sparkSession.sparkContext.emptyRDD[Row].setName("empty"),
          StructType(userSpecifiedPartitionSchema.get.fields ++ sinkStatusFields))
      (None, emptyDataFrame)
    }
  }

  def readDF(latestBatchId: Long): DataFrame = {
    val batchMetadataFile = batchIdToPath(latestBatchId)
    logInfo("Reading for batchid : " + latestBatchId + " : " + batchMetadataFile)
    sparkSession.read
      .format(format)
      .option("inside_metatdata_dir", "true")
      .load(batchMetadataFile.toUri.toString)
  }

  val partitionSchema = {
    val dfSchema = currentBatchAndDf._2.schema
    val partitionSchemaFields = dfSchema.fields.size - sinkStatusFields.size
    StructType(dfSchema.fields.splitAt(partitionSchemaFields)._1)
  }

  val logFields = partitionSchema.fields ++ sinkStatusFields

  private def getAndUpdateLatestDataFrame(): DataFrame = {
    val latestBatchId = getLatestBatchId()
    synchronized {
      if (latestBatchId != this.currentBatchAndDf._1) {
        currentBatchAndDf = (latestBatchId, readDF(latestBatchId.get))
      }
      return currentBatchAndDf._2
    }
  }

  def getPartitionSchema(): StructType = {
    partitionSchema
  }

  def inputFiles: Array[String] = {
    val df = getAndUpdateLatestDataFrame()
    import sparkSession.implicits._
    df.select("path").as[String].collect()
  }

  def allFiles(): Seq[FileStatus] = {
    val df = getAndUpdateLatestDataFrame()
    import sparkSession.implicits._
    df.select(sinkStatusFields.head.name, sinkStatusFields.tail.map(_.name): _*)
      .as[SinkFileStatus].collect().map(x => x.toFileStatus)
  }

  def sizeInBytes: Long = {
    val df = getAndUpdateLatestDataFrame()
    import sparkSession.implicits._
    df.selectExpr("ifnull(sum(size), cast(0 as bigint))").as[Long].first()
  }

  def listFiles(filter: Seq[Expression]): Seq[PartitionDirectory] = {
    import sparkSession.implicits._
    val df: DataFrame = getAndUpdateLatestDataFrame

    val filterStr = filter.map(_.sql).mkString(" and ")

    val filterDf = if (filter.size == 0) {
      df
    } else {
      df.filter(filterStr)
    }

    val partitionCols = partitionSchema.fields.map(_.name)

    if (partitionSchema.fields.size > 0) {
      val res = filterDf
        .groupBy(partitionCols.head, partitionCols.tail: _*)
        .agg(expr(sinkStatusStructStr))
        .collect()

      res.map { ps =>
        assert(ps.toSeq.size == partitionCols.size + 1)
        val (partitions, array) = ps.toSeq.splitAt(partitionCols.size)
        val elems = partitions
          .map(x => internalRowType(x))
        PartitionDirectory(InternalRow(elems: _*),
          array(0).asInstanceOf[mutable.WrappedArray[Row]].map { r =>
            val rr = r.asInstanceOf[Row]
            val sf: SinkFileStatus =
              new SinkFileStatus(rr.getLong(0), rr.getString(1), rr.getLong(2), rr.getBoolean(3),
                rr.getLong(4), rr.getInt(5), rr.getLong(6), rr.getString(7))
            sf.toFileStatus
          })
      }.toSeq
    } else {
      val files = filterDf.as[SinkFileStatus].collect()
      Seq(PartitionDirectory(InternalRow(), files.map(_.toFileStatus)))
    }
  }


  private def internalRowElemToData(elem: Any): Any = {
    elem match {
      case ad: GenericArrayData => ad.array
      case str: UTF8String => str.toString
      case y => y
    }
  }

  private def internalRowType(elem: Any): Any = {
    elem match {
      case a: mutable.WrappedArray[_] => a
      case struct: InternalRow => struct
      case x => Literal(x).value
    }
  }


  override def add(batchId: Long, metadata: Array[SinkFileStatus]): Boolean = {
    val caseInsensitiveOptions = CaseInsensitiveMap(parameters)
    val timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
      .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)

    val newDf = if (partitionSchema.fields.size == 0) {
      sparkSession.createDataFrame(metadata)
    } else {
      val rows = metadata.map { m =>
        val PartitionSpec(_, partitions) = PartitioningUtils.parsePartitions(
          Seq(new Path(m.path)),
          typeInference = false,
          basePaths = Set(new Path(baseDataPath)),
          userSpecifiedSchema = Some(partitionSchema),
          caseSensitive = false,
          timeZoneId = timeZoneId)
        val partitionData = partitions.head.values.toSeq(partitionSchema).map { elem =>
          internalRowElemToData(elem)
        }
        val cols = partitionData ++ m.productIterator
        Row.apply(cols: _*)
      }
      val rdd = sparkSession.sparkContext.parallelize(rows)
      sparkSession.createDataFrame(rdd, StructType(logFields))
    }
    val toWrite = newDf.union(getAndUpdateLatestDataFrame())
    val latestBatchId = getLatestBatchId()
    val newBatchID = latestBatchId.getOrElse(-1L) + 1
    val newPath = batchIdToPath(newBatchID)
    logDebug("Adding for batchid : " + newBatchID + " Rows : " + metadata.size)
    toWrite.write.format(format).save(newPath.toUri.toString)
    logInfo("Added for batchid : " + newBatchID + " Rows : " + metadata.size)
    true
  }


  /**
   * Return the metadata for the specified batchId if it's stored. Otherwise, return None.
   */
  override def get(batchId: Long): Option[Array[SinkFileStatus]] = {
    import org.apache.spark.sql.Encoders
    val sflEncoder = Encoders.product[SinkFileStatus]
    val df = getPartitonAndSinkFileStatusInternal(batchId)
    val cols = logFields.map(_.name)
    val res = df.select(cols.head, cols.tail: _*).as[SinkFileStatus](sflEncoder).collect()
    if (res.isEmpty) {
      None
    } else {
      Some(res)
    }
  }

  /**
   * Return metadata for batches between startId (inclusive) and endId (inclusive). If `startId` is
   * `None`, just return all batches before endId (inclusive).
   */

  override def get(startId: Option[Long],
                   endId: Option[Long]): Array[(Long, Array[SinkFileStatus])] = {
    val df = getPartitionAndSinkFileStatusInternal(startId, endId)
    val sflEncoder = Encoders.product[(Long, Array[SinkFileStatus])]
    val cols = logFields.map(_.name)
    val res = df.select(cols.head, cols.tail: _*).groupBy("batchId")
      .agg(expr(sinkStatusStructStr))
      .as[(Long, Array[SinkFileStatus])](sflEncoder).collect()
    res
  }

  def allFilesWithLatestBatchId(): Option[(Long, Array[SinkFileStatus])] = {
    val latestBatchId = getLatestBatchId()
    get(latestBatchId, latestBatchId).headOption
  }

  private def getPartitonAndSinkFileStatusInternal(batchId: Long) = {
    val df = getAndUpdateLatestDataFrame()
    df.filter(s"$batchIdColName=$batchId")
  }

  /**
   * Return metadata for batches between startId (inclusive) and endId (inclusive).
   * If `startId` is `None`, just return all batches before endId (inclusive).
   */
  def getPartitionAndSinkFileStatus(startId: Option[Long], endId: Option[Long]): Array[Row] = {
    getPartitionAndSinkFileStatusInternal(startId, endId).collect();
  }

  private def getPartitionAndSinkFileStatusInternal(startId: Option[Long],
                                                    endId: Option[Long]) = {
    val df = getAndUpdateLatestDataFrame()
    val filter =
      (startId, endId) match {
        case (Some(s), Some(e)) =>
          df.filter(s"$batchIdColName>=$s").filter(s"batchid<=$e")
        case (None, None) => df
        case (Some(s), None) => df.filter(s"$batchIdColName>=$s")
        case (None, Some(e)) => df.filter(s"$batchIdColName<=$e")
      }
    filter
  }
}
