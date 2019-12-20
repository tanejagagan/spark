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

import java.util.{Collections, Optional}

import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient
import org.apache.kafka.common.{Node => KNode}

import scala.collection.JavaConverters._
import org.apache.kafka.common.{TopicPartition}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata
import org.apache.spark.internal.Logging


case class ResponseOffset(offset: Long, timestamp: Long)

class KafkaException(ex: Exception) extends RuntimeException {
  def this(str: String) = this(new Exception(str))

  def this(str: String, exception: Exception) = this(exception)
}


object KafkaUnsafeFetcher extends Logging {

  def getPartitions(connection: ConsumerNetworkClient, topic: String): Seq[PartitionMetadata] = {
    try {
      val response = metadataFor(connection, Seq(topic))
      response.topicMetadata().iterator().next().partitionMetadata().asScala
    } catch {
      case ex: Exception => throw new KafkaException(ex)
    }
  }

  def metadataFor(connection: ConsumerNetworkClient, topics: Seq[String]): MetadataResponse = {
    val requestBuilder =
      new MetadataRequest.Builder(topics.asJava, false)
    val future = connection.send(connection.leastLoadedNode(), requestBuilder)
    connection.poll(future)
    future.value().responseBody().asInstanceOf[MetadataResponse]
  }


  private def getOffsets(connection: ConsumerNetworkClient,
                         partitionMetadata: Seq[PartitionMetadata],
                         targetTime: Map[TopicPartition, Long]):
  Map[TopicPartition, ResponseOffset] = {
    val leaderMap: Map[Int, KNode] = partitionMetadata.map { x =>
      (x.partition(), x.leader())
    }.toMap

    // Group by node
    val nodeTpMap = targetTime.toSeq.map { case (tp, time) =>
      val leaderNode = leaderMap(tp.partition())
      (leaderNode, (tp, time))
    }.groupBy(_._1)

    val futures = nodeTpMap.map { case (node, seq) =>
      val nodeTargetTime: Map[TopicPartition, Long] = seq.map(_._2).toMap
      val builder = ListOffsetRequest.Builder.forConsumer(true, IsolationLevel.READ_COMMITTED)
        .setTargetTimes(nodeTargetTime.map(x =>
          (x._1, new ListOffsetRequest.PartitionData(x._2,
            Optional.empty().asInstanceOf[Optional[Integer]]))).asJava)
      connection.send(node, builder)
    }
    futures.foreach { req =>
      connection.poll(req)
    }

    futures.flatMap(req =>
      req.value().responseBody()
        .asInstanceOf[ListOffsetResponse]
        .responseData()
        .asScala
        .map { case (tp, pd) =>
          (tp, ResponseOffset(pd.offset, pd.timestamp))
        }
    ).toMap
  }

  def latestOffsets(connection: ConsumerNetworkClient,
                    partitionMetadata: Seq[PartitionMetadata],
                    topicPartitions: Seq[TopicPartition]): Map[TopicPartition, ResponseOffset] = {
    val targetTimes = topicPartitions.map(t => t -> -1L).toMap
    getOffsets(connection, partitionMetadata, targetTimes)
  }

  def earliestOffsets(connection: ConsumerNetworkClient,
                      partitionMetadata: Seq[PartitionMetadata],
                      topicPartitions: Seq[TopicPartition]): Map[TopicPartition, ResponseOffset] = {
    val targetTimes = topicPartitions.map(t => t -> -2L).toMap
    getOffsets(connection, partitionMetadata, targetTimes)
  }

  def offsetsForTimestamp(connection: ConsumerNetworkClient,
                          partitionMetadata: Seq[PartitionMetadata],
                          timestampMap: Map[TopicPartition, Long]):
  Map[TopicPartition, ResponseOffset] = {
    getOffsets(connection, partitionMetadata, timestampMap)
  }

  def sendFetch(consumerClient: ConsumerNetworkClient,
                node: KNode,
                topicPartition: TopicPartition,
                startFetchOffset: Long,
                endFetchOffset: Long,
                maxSize: Int,
               ): ClientResponse = {
    val fetchData: java.util.Map[TopicPartition, FetchRequest.PartitionData] =
      Collections.singletonMap(topicPartition, new FetchRequest.PartitionData(startFetchOffset,
        startFetchOffset, maxSize, Optional.empty()))
    val fetchRequest = FetchRequest.Builder.forConsumer(0, 1,
      fetchData).metadata(FetchMetadata.INITIAL)

    val requestFuture = consumerClient.send(node, fetchRequest)
    consumerClient.poll(requestFuture)
    requestFuture.value()
  }
}
