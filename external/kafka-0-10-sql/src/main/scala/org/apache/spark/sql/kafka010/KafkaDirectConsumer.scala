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
import java.nio.channels.SocketChannel
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.{RecordBatch, Records}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.kafka.common.utils.AbstractIterator

import org.apache.spark.internal.Logging


case class ResponseOffset(offset: Long, timestamp: Long)

class KafkaException(ex: Exception ) extends RuntimeException {
  def this(str : String ) = this(new Exception(str))
  def this(str : String, exception : Exception) = this(exception)
}


object KafkaDirectConsumer extends Logging {

  def getPartitions(kafkaConnection: KafkaConnection, topic: String): Seq[PartitionMetadata] = {
    try {
      val response = metadataFor(kafkaConnection, Seq(topic))
      response.topicMetadata().iterator().next().partitionMetadata().asScala
    } catch {
      case ex: Exception => throw new KafkaException(ex)
    }
  }

  def metadataFor(kafkaConnection: KafkaConnection, topics: Seq[String]): MetadataResponse = {
    try {
      val clientId = kafkaConnection.clientId
      val correlationId = kafkaConnection.correlationIdCounter.getAndIncrement()
      val requestHeader = new RequestHeader(ApiKeys.METADATA,
        ApiKeys.METADATA.latestVersion(),
        clientId, correlationId)
      val hostPort = kafkaConnection.hostPort
      val socketChannel = kafkaConnection.channel
      val responseBuffer = ByteBuffer.allocate(topics.size * 4096)
      val request: MetadataRequest =
        new MetadataRequest.Builder(topics.asJava, false).build(ApiKeys.METADATA.latestVersion())
      val send = request.toSend(hostPort, requestHeader)
      send.writeTo(socketChannel)
      readResponse(socketChannel, responseBuffer)
      val responseBody = parseResponse(requestHeader, responseBuffer)
      responseBody.asInstanceOf[MetadataResponse]
    }
    catch {
      case ex: Exception => throw new KafkaException(ex)
    }
  }

  def latestOffsets(kafkaConnection: KafkaConnection,
                    topicPartitions: Seq[TopicPartition]): Map[TopicPartition, ResponseOffset] = {
    val targetTimes = topicPartitions.map(t => t -> -1L).toMap
    getOffsets(kafkaConnection, targetTimes)
  }

  private def getOffsets(kafkaConnection: KafkaConnection,
                         targetTimes: Map[TopicPartition, Long]):
  Map[TopicPartition, ResponseOffset] = {
    getOffsets(kafkaConnection.channel, kafkaConnection.clientId,
      kafkaConnection.correlationIdCounter.getAndIncrement(), kafkaConnection.hostPort,
      ByteBuffer.allocate(4096), targetTimes)
  }

  private def getOffsets(
                          socketChannel: SocketChannel,
                          clientId: String,
                          correlationId: Int,
                          hostPort: String,
                          responseBuffer: ByteBuffer,
                          targetTime: Map[TopicPartition, Long]):
    Map[TopicPartition, ResponseOffset] = {
    try {
      val requestHeader = new RequestHeader(ApiKeys.LIST_OFFSETS,
        ApiKeys.LIST_OFFSETS.latestVersion(),
        clientId, correlationId)

      import scala.collection.JavaConverters._
      val request: ListOffsetRequest =
        ListOffsetRequest.Builder.forConsumer(true, IsolationLevel.READ_COMMITTED)
          .setTargetTimes(targetTime.map(x =>
            (x._1, new ListOffsetRequest.PartitionData(x._2,
              Optional.empty().asInstanceOf[Optional[Integer]]))).asJava)
          .build()
      val send = request.toSend(hostPort, requestHeader)
      send.writeTo(socketChannel)

      readResponse(socketChannel, responseBuffer)
      val responseBody = parseResponse(requestHeader, responseBuffer)
      val response = responseBody.asInstanceOf[ListOffsetResponse]
      response.responseData().asScala.map { case (tp, pd) =>
        (tp, ResponseOffset(pd.offset, pd.timestamp))
      }.toMap
    } catch {
      case ex: Exception => throw new KafkaException(ex)
    }
  }

  def earliestOffsets(kafkaConnection: KafkaConnection,
                      topicPartitions: Seq[TopicPartition]): Map[TopicPartition, ResponseOffset] = {
    val targetTimes = topicPartitions.map(t => t -> -2L).toMap
    getOffsets(kafkaConnection, targetTimes)
  }

  def offsetsForTimestamp(kafkaConnection: KafkaConnection,
                          timestampMap: Map[TopicPartition, Long]):
  Map[TopicPartition, ResponseOffset] = {
    getOffsets(kafkaConnection, timestampMap)
  }

  def sendFetch(socketChannel: SocketChannel,
                clientId: String,
                correlationId: Int,
                hostPort: String,
                topicPartition: TopicPartition,
                startFetchOffset: Long,
                endFetchOffset: Long,
                responseBuffer: ByteBuffer):
  (RequestHeader, FetchResponse.PartitionData[Records]) = {
    try {

      val requestHeader = new RequestHeader(ApiKeys.FETCH, ApiKeys.FETCH.latestVersion(),
        clientId, correlationId)
      val fetchData: java.util.Map[TopicPartition, FetchRequest.PartitionData] =
        Collections.singletonMap(topicPartition, new FetchRequest.PartitionData(startFetchOffset,
          startFetchOffset, responseBuffer.capacity(), Optional.empty()))
      val fetchRequest = FetchRequest.Builder.forConsumer(0, 1,
        fetchData).metadata(FetchMetadata.INITIAL)

      val send = fetchRequest
        .build(ApiKeys.FETCH.latestVersion())
        .toSend(hostPort, requestHeader)

      send.writeTo(socketChannel)
      readResponse(socketChannel, responseBuffer)
      val responseBody = parseResponse(requestHeader, responseBuffer)
      val response = responseBody.asInstanceOf[FetchResponse[Records]]

      (requestHeader, response.responseData().get(topicPartition))
      // topicPartitionEntries.records.batchIterator()
    } catch {
      case ex: Exception => throw new KafkaException(ex)
    }
  }

  private def readResponse(socketChannel: SocketChannel,
                           responseBuffer: ByteBuffer) {
    responseBuffer.rewind()
    responseBuffer.limit(4)
    socketChannel.read(responseBuffer)
    val size = responseBuffer.getInt(0)
    responseBuffer.clear()
    var totalRead = 0
    responseBuffer.rewind()
    assert(responseBuffer.remaining() >= size, "Response buffer size is smaller than bytes read")
    while (totalRead < size) {
      val len = socketChannel.read(responseBuffer)
      totalRead += len
    }
    responseBuffer.flip()
  }

  def parseResponse(requestHeader: RequestHeader, responseBuffer: ByteBuffer): AbstractResponse = {
    val header = ResponseHeader.parse(responseBuffer)
    val responseStruct = requestHeader.apiKey().parseResponse(requestHeader.apiVersion(),
      responseBuffer)
    AbstractResponse.parseResponse(requestHeader.apiKey, responseStruct, requestHeader.apiVersion)
  }
}
