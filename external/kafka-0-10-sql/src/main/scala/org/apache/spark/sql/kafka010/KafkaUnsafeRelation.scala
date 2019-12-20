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

import java.util.{Locale, Properties}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient
import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.{BaseRelation, CatalystScan, DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object KafkaUnsafeRelation {
  val kafkaPartitionCol = "partition"
  val kafkaOffsetCol = "offset"
  val kafkaTopicCol = "topic"
  val kafkaTimestampCol = "timestamp"
  val EARLIEST_OFFSET = -2L
  val LATEST_OFFSET = -1L

  val fixedSchema = StructType(
    Seq(
      StructField(kafkaTopicCol, StringType),
      StructField(kafkaPartitionCol, IntegerType),
      StructField(kafkaOffsetCol, LongType),
      StructField(kafkaTimestampCol, LongType)))

  /*
   * 1. Filter if there are any partition filters. Eg partition in (1, 2, 3) and partition = 1
   * 2. Get the offsets
   *
   */
  def resolvePartitionsAndOffsets(consumerClient : ConsumerNetworkClient,
                                  topic: String,
                                  partitions: Option[Seq[Int]],
                                  filters: Seq[Expression]):
  Seq[KafkaUnsafeSourceRDDOffsetRange] = {

    val (timeEarliestOffsetBound, timeLatestOffsetBounds) = timeBounds(filters)
    val ob = offsetBounds(topic, filters)
    val earliestOffsetBound: Option[Map[TopicPartition, Long]] = ob._1
    val latestOffsetBounds: Option[Map[TopicPartition, Long]] = ob._2

      val partitionSet = partitions.map( _.toSet)
      val allPartitions = KafkaUnsafeFetcher.getPartitions(consumerClient, topic).filter{ pm =>
        partitionSet match {
          case Some(x) => x.contains(pm.partition())
          case None => true
        }
      }.map( pm => pm.partition()-> pm).toMap
      val filteredPartitionSeq = filterPartitions(filters, allPartitions.map(p => p._1).toSeq)
      val filteredPartition = filteredPartitionSeq.map( allPartitions(_))
      val filteredTopicPartition = filteredPartition.map(a => new TopicPartition(topic,
        a.partition()))

      val offsetToFetch = filteredTopicPartition.map { tp =>
        (tp -> timeEarliestOffsetBound.getOrElse(EARLIEST_OFFSET),
          tp -> timeLatestOffsetBounds.getOrElse(LATEST_OFFSET))
      }

      val earliestFromBroker = KafkaUnsafeFetcher
        .offsetsForTimestamp(consumerClient, allPartitions.map(_._2).toSeq,
          offsetToFetch.map { case (e, l) => e }.toMap)

      val earliestOffsets = earliestFromBroker
        .map { case (tp, roffset) =>
          val ob = earliestOffsetBound.getOrElse(Map()).getOrElse(tp, roffset.offset)
          tp -> Math.max(roffset.offset, ob)
        }

      val latestFromBroker = KafkaUnsafeFetcher
        .offsetsForTimestamp(consumerClient, allPartitions.map(_._2).toSeq,
          offsetToFetch.map { case (e, l) => l }.toMap)

      val latestOffsets = latestFromBroker
        .map { case (tp, roffset) =>
          val ob = latestOffsetBounds.getOrElse(Map()).getOrElse(tp, roffset.offset)
          tp -> Math.min(roffset.offset, ob)
        }

      filteredTopicPartition.map { tp =>
        val meta = allPartitions(tp.partition())
        KafkaUnsafeSourceRDDOffsetRange(new Node(meta.leader()),
          meta.isr().asScala.map(new Node(_)),
          tp, earliestOffsets(tp), latestOffsets(tp), None)
      }
  }

  def filterPartitions(filters : Seq[Expression],
                       allPartitions : Seq[Int]): Seq[Int] = {
    val applicableFilters = applicablePartitionFilters( filters )
    applicableFilters.foldLeft(allPartitions.map( p => InternalRow(p)))
    { case (fPartitions, expression) =>
      val pExpr = InterpretedPredicate.create(expression)
      fPartitions.filter { ff =>
        pExpr.eval(ff)
      }
    }.map( x => x.getInt(0))
  }

  private def applicablePartitionFilters(filters : Seq[Expression]) = {
    filters.filter { p =>
      p.references.size == 1 && p.references.head.isInstanceOf[AttributeReference] &&
        p.references.head.asInstanceOf[AttributeReference].name == kafkaPartitionCol
    }.map { filter =>
      filter.transform{
        case AttributeReference(name, dataType, nullable, metadata) =>
          BoundReference(0, dataType, nullable)
      }
    }
  }

  def timeBounds(filters : Seq[Expression]) : (Option[Long], Option[Long]) = {
    def resolve(one : Option[Long], two : Option[Long], fn : (Long, Long) => Long): Option[Long] = {
      (one, two) match {
        case (None, None) => None
        case (Some(x), None ) => Some(x)
        case (None, Some(x)) => Some(x)
        case (Some(x), Some(y)) => Some(fn(x, y))
      }
    }
    val timeFilters = filters.filter { p =>
      p.references.size == 1 && p.references.head.isInstanceOf[Attribute] &&
        p.references.head.asInstanceOf[Attribute].name == kafkaTimestampCol
    }

    val bounds = timeFilters.map { x =>
      x match {
        case EqualTo(a : Attribute, l ) if (a.name == kafkaTimestampCol) =>
          (Some(Cast(l, LongType).eval(null).asInstanceOf[Long]),
            Some(Cast(l, LongType).eval(null).asInstanceOf[Long] + 1))
        case EqualTo(l, a : Attribute) if (a.name == kafkaTimestampCol) =>
          (Some(Cast(l, LongType).eval(null).asInstanceOf[Long]),
            Some(Cast(l, LongType).eval(null).asInstanceOf[Long] + 1))

        // Lower bound
        case LessThan(l, a : Attribute) if (a.name == kafkaTimestampCol) =>
          (Some(Cast(l, LongType).eval(null).asInstanceOf[Long]), None)
        case LessThanOrEqual(l, a : Attribute) if (a.name == kafkaTimestampCol) =>
          (Some(Cast(l, LongType).eval(null).asInstanceOf[Long]), None)
        case GreaterThan(a : Attribute, l) if (a.name == kafkaTimestampCol) =>
          (Some(Cast(l, LongType).eval(null).asInstanceOf[Long]), None)
        case GreaterThanOrEqual(a : Attribute, l) if (a.name == kafkaTimestampCol) =>
          (Some(Cast(l, LongType).eval(null).asInstanceOf[Long]), None)

        // Upper bound
        case LessThan(a : Attribute, l) if (a.name == kafkaTimestampCol) =>
          (None, Some(Cast(l, LongType).eval(null).asInstanceOf[Long] + 1))
        case LessThanOrEqual(a : Attribute, l) if (a.name == kafkaTimestampCol) =>
          (None, Some(Cast(l, LongType).eval(null).asInstanceOf[Long] + 1))
        case GreaterThan(l, a : Attribute) if (a.name == kafkaTimestampCol) =>
          (None, Some(Cast(l, LongType).eval(null).asInstanceOf[Long] + 1))
        case GreaterThanOrEqual(l, a : Attribute) if (a.name == kafkaTimestampCol) =>
          (None, Some(Cast(l, LongType).eval(null).asInstanceOf[Long] + 1))
        case _ => (None, None)
      }
    }
    bounds.foldLeft[(Option[Long], Option[Long])]((None, None)) { case (start, current) =>
      (resolve(start._1, current._1, (o1, o2) => Math.max(o1, o2)),
      resolve(start._2, current._2, (o1, o2) => Math.min(o1, o2)))
    }
  }

  private def offsetBounds(topic : String,
                           filters : Seq[Expression])
  : (Option[Map[TopicPartition, Long]], Option[Map[TopicPartition, Long]]) = {
    val offsetExpressions = filters.filter { x =>
      x match {
        case OffsetBounds(AttributeReference(kafkaPartitionCol, _, _, _),
        AttributeReference(kafkaOffsetCol, _, _, _), lowerBound, upperBound) => true
        case _ => false
      }
    }
    if(offsetExpressions.size > 1) {
      throw new RuntimeException("At most on OffsetBound expression is supported")
    }


    val offsetBounds = offsetExpressions.headOption.map(_.asInstanceOf[OffsetBounds])

    val latestOffsetBounds = offsetBounds.flatMap( oob =>
      oob.getUpperBounds match {
        case None => None
        case Some(ob) =>
          val map = ob.map{ case(partition, offset) =>
          new TopicPartition(topic, partition )-> offset
          }.toMap
          Some(map)
      })
    val earliestOffsetBound = offsetBounds.flatMap( oob =>
      oob.getLowerBounds match {
        case None => None
        case Some(ob) =>
          val map = ob.map{ case(partition, offset) =>
            new TopicPartition(topic, partition )-> offset
          }.toMap
          Some(map)
      })
    (earliestOffsetBound, latestOffsetBounds)
  }
}

private[kafka010] class KafkaUnsafeRelation(
    override val sqlContext: SQLContext,
    sourceOptions: Map[String, String],
    specifiedKafkaParams: Map[String, String],
    keySchema : StructType,
    valueSchema : StructType,
    topic : String,
    partitions : Option[Seq[Int]]) extends BaseRelation with CatalystScan with Logging {

  override val schema: StructType = StructType(KafkaUnsafeRelation.fixedSchema.fields ++
    keySchema.fields ++ valueSchema.fields)
  private val pollTimeoutMs = sourceOptions.getOrElse(
    "kafkaConsumer.pollTimeoutMs",
    (sqlContext.sparkContext.conf.getTimeAsSeconds(
      "spark.network.timeout",
      "120s") * 1000L).toString
  ).toLong


  override def buildScan(
    requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    val nameIndexMap = schema.fields.map(_.name).zipWithIndex.toMap
    val projectMapping = Some(requiredColumns.map(x => x.name)
      .map(c => nameIndexMap(c).toInt).toArray)

    // Create an RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val executorKafkaParams =
      KafkaSourceProvider.kafkaParamsForExecutors(specifiedKafkaParams, "uniqueGroupId")

    val bootstrapServers = new java.util.ArrayList[String]()
    val bootstrapServerStr = executorKafkaParams.get("bootstrap.servers")
    bootstrapServerStr.toString.split(",").foreach( bootstrapServers.add(_))

    val props = new Properties()
    props.putAll(executorKafkaParams)
    val consumerClient = KafkaConsumerClientRegistry.INSTANCE.getOrCreate(
      bootstrapServers, props
    )
    val offsets = KafkaUnsafeRelation.resolvePartitionsAndOffsets(consumerClient,
      topic, partitions, filters)

    val rdd = new KafkaUnsafeSourceRDD(
      sqlContext.sparkContext, executorKafkaParams, offsets.toSeq, keySchema.size,
      pollTimeoutMs, true, reuseKafkaConsumer = false, projectMapping)
      rdd.asInstanceOf[RDD[Row]]
  }

  override def toString: String =
    s"KafkaUnsafeRelation"

  override def needConversion: Boolean = false
}

class KafkaUnsafeRelationProvider extends DataSourceRegister with SchemaRelationProvider {
  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = "kafka-unsafe"

  /**
   * Returns a new base relation with the given parameters and user defined schema.
   *
   * @note The parameters' keywords are case insensitive and this insensitivity is enforced
   *       by the Map that is passed to the function.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String], schema: StructType): BaseRelation = {
    val schemaMap = schema.fields.map(f => (f.name, f)).toMap
    val keySchema = parameters.get("key-columns").map{ list =>
      val fields = list.split(",").map( _.trim).map(name => schemaMap(name))
      StructType(fields)
    }.getOrElse(new StructType())
    val keySchemaFields = keySchema.fields.map(_.name).toSet
    val fixedSchemaFields = KafkaUnsafeRelation.fixedSchema.fields.map(_.name).toSet
    val valueSchemaFields = schema.fields.filterNot(f => keySchemaFields.contains(f.name))
      .filterNot(f => fixedSchemaFields.contains(f.name))

    val valueSchema = StructType(valueSchemaFields)
    val partitions = parameters.get("partitions").map( _.split(",").map(_.toInt).toSeq)
    val topic = parameters("topic")
    val startingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(
      parameters, KafkaSourceProvider.STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit)
    assert(startingRelationOffsets != LatestOffsetRangeLimit)

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase(Locale.ROOT).startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val endingRelationOffsets = KafkaSourceProvider.getKafkaOffsetRangeLimit(parameters,
      KafkaSourceProvider.ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
    assert(endingRelationOffsets != EarliestOffsetRangeLimit)
    new KafkaUnsafeRelation(sqlContext, caseInsensitiveParams, specifiedKafkaParams, keySchema,
      valueSchema, topic, partitions)
  }
}
