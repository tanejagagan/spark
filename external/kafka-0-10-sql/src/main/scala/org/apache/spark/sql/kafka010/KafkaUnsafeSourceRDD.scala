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
import java.{util => ju}

import org.apache.kafka.clients.consumer.internals.FetcherMetricsRegistry
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.{Record, RecordBatch, Records}
import org.apache.kafka.common.requests.{FetchResponse, RequestHeader}
import org.apache.kafka.common.utils.AbstractIterator

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.NextIterator



case class KafkaUnsafeSourceRDDOffsetRange( partitionLeader : String,
                                            isr : Seq[String],
                                            topicPartition: TopicPartition,
                                            fromOffset: Long,
                                            untilOffset: Long,
                                            preferredLoc: Option[String]) {
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition
  def size: Long = untilOffset - fromOffset
}

case class KafkaUnsafeSourceRDDPartition( index: Int,
                                          offsetRange: KafkaUnsafeSourceRDDOffsetRange)
  extends Partition
/**
 * An RDD that reads data from Kafka based on offset ranges across multiple partitions.
 * Additionally, it allows preferred locations to be set for each topic + partition, so that
 * the [[KafkaSource]] can ensure the same executor always reads the same topic + partition
 * and cached KafkaConsumers (see [[KafkaDataConsumer]] can be used read data efficiently.
 *
 * @param sc the [[SparkContext]]
 * @param executorKafkaParams Kafka configuration for creating KafkaConsumer on the executors
 * @param offsetRanges Offset ranges that define the Kafka data belonging to this RDD
 */
private[kafka010] class KafkaUnsafeSourceRDD(
    sc: SparkContext,
    executorKafkaParams: ju.Map[String, Object],
    offsetRanges: Seq[KafkaUnsafeSourceRDDOffsetRange],
    numKeyFields : Int,
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    reuseKafkaConsumer: Boolean,
    projectionMapping : Option[Array[Int]])
  extends RDD[InternalRow](sc, Nil) {

  val defaultBufferSize = 64 * 1024 * 1024
  val bootstrapServers = new java.util.ArrayList[String]()
  val bootstrapServerStr = executorKafkaParams.get("bootstrap.servers")
  bootstrapServerStr.toString.split(",").foreach( bootstrapServers.add(_))
  val startClientId = "unsafe-row-reader"
  val bufferSize = executorKafkaParams.getOrDefault("read-buffer-size", defaultBufferSize.toString)


  override def persist(newLevel: StorageLevel): this.type = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) => new KafkaUnsafeSourceRDDPartition(i, o) }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val part = split.asInstanceOf[KafkaUnsafeSourceRDDPartition]
    part.offsetRange.preferredLoc.map(Seq(_)).getOrElse(Seq.empty)
  }

  override def compute(
      thePart: Partition,
      context: TaskContext): Iterator[InternalRow] = {

    val sourcePartition = thePart.asInstanceOf[KafkaUnsafeSourceRDDPartition]


    val range = sourcePartition.offsetRange
    assert(
      range.fromOffset >= 0 && range.untilOffset >= 0 &&
      range.fromOffset <= range.untilOffset,
      s"Beginning offset ${range.fromOffset} is after the ending offset ${range.untilOffset} " +
        s"for topic ${range.topic} partition ${range.partition}. " +
        "You either provided an invalid fromOffset, or the Kafka topic has been damaged")
    if (range.fromOffset == range.untilOffset) {
      logInfo(s"Beginning offset ${range.fromOffset} is the same as ending offset " +
        s"skipping ${range.topic} ${range.partition}")
      Iterator.empty
    } else {
      val underlying = new KafkaUnsafeIterator(
        bufferSize.toString.toInt, sourcePartition,
        numKeyFields,
        bootstrapServers, projectionMapping)

      // Release consumer, either by removing it or indicating we're no longer using it
      context.addTaskCompletionListener[Unit] { _ =>
        underlying.closeIfNeeded()
      }
      underlying
    }
  }
}

@deprecated("use KafkaUnsafeIterator")
class KafkaUnsafeIteratorDeprecated(bufferSize : Int,
                          sourcePartition : KafkaUnsafeSourceRDDPartition,
                          numKeyFields : Int,
                          bootstrapServers : java.util.List[String],
                          projectionMapping : Option[Array[Int]] = None)
  extends NextIterator[InternalRow]() {

  @transient lazy val buffer = ByteBuffer.allocate(bufferSize.toString.toInt)
  val range = sourcePartition.offsetRange
  val startClientId = "unsafe-row-iterator"
  val kafkaConnectionPool = KafkaConnectionPool.getOrCreate(10,
    new KafkaConnectionPoolConfig(bootstrapServers, startClientId));
  var requestOffset = range.fromOffset
  var nextBatches = getNextBatches()
  var nextBatch = getNextBatch()
  val kafkaUnsafeRow = new KafkaUnsafeRow(sourcePartition.offsetRange.topic,
    sourcePartition.offsetRange.partition, numKeyFields)
  val kafkaProjectedUnsafeRow = projectionMapping.map { p =>
    val pp = new KafkaProjectedUnsafeRow(p)
    pp.pointsTo(kafkaUnsafeRow)
    pp
  }.getOrElse(kafkaUnsafeRow)


  def getNextBatches() : AbstractIterator[_<:RecordBatch] = {
    var res : AbstractIterator[_<:RecordBatch] = null
    val connectionMeta = kafkaConnectionPool.getConnection
    try {
      res = KafkaDirectConsumer.sendFetch(connectionMeta.channel, connectionMeta.hostPort,
        connectionMeta.correlationIdCounter.getAndIncrement(), connectionMeta.hostPort,
        sourcePartition.offsetRange.topicPartition, requestOffset,
        sourcePartition.offsetRange.untilOffset, buffer)._2.records.batchIterator()
    } catch {
      case ex : Exception => kafkaConnectionPool.markForRecovery(connectionMeta)
        throw new RuntimeException(ex)
    } finally {
      kafkaConnectionPool.releaseConnection(connectionMeta)
    }
    res ;
  }

  def getNextBatch() : java.util.Iterator[Record] = {
    if (nextBatches.hasNext) {
      nextBatches.next().iterator()
    } else {
      nextBatches = getNextBatches()
      nextBatches.next().iterator()
    }
  }

  private def readNext = {
    if (nextBatch.hasNext()) {
      nextBatch.next()
    } else {
      nextBatch = getNextBatch()
      nextBatch.next()
    }
  }


  override def getNext(): InternalRow = {
    if (requestOffset >= range.untilOffset) {
      // Processed all offsets in this partition.
      finished = true
      null
    } else {
      var r = readNext
      while(r.offset() < requestOffset) {
        r = readNext
      }

      if (r == null) {
        // Losing some data. Skip the rest offsets in this partition.
        finished = true
        null
      } else {
        requestOffset = r.offset + 1
        kafkaUnsafeRow.pointsTo(r.offset(), r.timestamp(),
          !r.hasKey, r.key(), !r.hasValue, r.value())
        // kafkaUnsafeRow
        kafkaProjectedUnsafeRow
      }
    }
  }

  override protected def close(): Unit = {

  }

}


class KafkaUnsafeIterator(bufferSize : Int,
                          sourcePartition : KafkaUnsafeSourceRDDPartition,
                          numKeyFields : Int,
                          bootstrapServers : java.util.List[String],
                          projectionMapping : Option[Array[Int]] = None)
  extends NextIterator[InternalRow]() {

  @transient lazy val buffer = ByteBuffer.allocate(bufferSize.toString.toInt)
  val range = sourcePartition.offsetRange
  val startClientId = "unsafe-row-iterator"
  val kafkaConnectionPool = KafkaConnectionPool.getOrCreate(10,
    new KafkaConnectionPoolConfig(bootstrapServers, startClientId));
  var requestOffset = range.fromOffset
  var currentPartitionRecords : PartitionRecords = _
  val kafkaUnsafeRow = new KafkaUnsafeRow(sourcePartition.offsetRange.topic,
    sourcePartition.offsetRange.partition, numKeyFields)
  val kafkaProjectedUnsafeRow = projectionMapping.map { p =>
    val pp = new KafkaProjectedUnsafeRow(p)
    pp.pointsTo(kafkaUnsafeRow)
    pp
  }.getOrElse(kafkaUnsafeRow)

  val registry = new FetcherMetricsRegistry()
  val metrics = new Metrics()
  val managerMetrics = new PartitionRecords.FetchManagerMetrics(metrics, registry)
  val partitionSet = {
    val set = new java.util.HashSet[TopicPartition]()
    set.add(sourcePartition.offsetRange.topicPartition)
    set
  }
  val agg = new PartitionRecords.FetchResponseMetricAggregator(managerMetrics, partitionSet)


  private def getNextBatchesV2 = {
    var res : (RequestHeader, FetchResponse.PartitionData[Records]) = null
    val connectionMeta = kafkaConnectionPool.getConnection
    try {
      res = KafkaDirectConsumer.sendFetch(connectionMeta.channel, connectionMeta.hostPort,
        connectionMeta.correlationIdCounter.getAndIncrement(), connectionMeta.hostPort,
        sourcePartition.offsetRange.topicPartition, requestOffset,
        sourcePartition.offsetRange.untilOffset, buffer)
    } catch {
      case ex : Exception => kafkaConnectionPool.markForRecovery(connectionMeta)
        throw new RuntimeException(ex)
    } finally {
      kafkaConnectionPool.releaseConnection(connectionMeta)
    }
    createPartitionRecords(res._2, requestOffset, res._1.apiVersion())
  }

  private def readNext = {
    if (currentPartitionRecords == null) {
      currentPartitionRecords = getNextBatchesV2
    }
    var next = currentPartitionRecords.nextFetchedRecord()
    if(next == null) {
      currentPartitionRecords = getNextBatchesV2
      next = currentPartitionRecords.nextFetchedRecord()
    }
    next
  }


  override def getNext(): InternalRow = {
    if (requestOffset >= range.untilOffset) {
      // Processed all offsets in this partition.
      finished = true
      null
    } else {
      var r = readNext
      while(r.offset() < requestOffset) {
        r = readNext
      }

      if (r == null) {
        // Losing some data. Skip the rest offsets in this partition.
        finished = true
        null
      } else {
        requestOffset = r.offset + 1
        kafkaUnsafeRow.pointsTo(r.offset(), r.timestamp(),
          !r.hasKey, r.key(), !r.hasValue, r.value())
        // kafkaUnsafeRow
        kafkaProjectedUnsafeRow
      }
    }
  }

  override protected def close(): Unit = {

  }


  private def createPartitionRecords(partitionData : FetchResponse.PartitionData[Records],
                                     fetchedOffset: Long,
                                     responseVersion: Short) = {

    val cFetch = new PartitionRecords.CompletedFetch(sourcePartition.offsetRange.topicPartition,
      fetchedOffset, partitionData,
      agg, responseVersion)

    val  batches = partitionData.records.batches().iterator()
    new PartitionRecords(sourcePartition.offsetRange.topicPartition,
      cFetch, batches)

  }
}




