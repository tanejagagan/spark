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
import java.util.{ArrayList => JArrayList, Properties }
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Or}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructType}


case class ResultVerify(valStr: String,
                        keyStr: String,
                        valInt: Int,
                        keyInt: Int,
                        topic: String,
                        partition: Int,
                        offset: Long,
                        valByte: Byte,
                        valShort: Short,
                        valLong: Long,
                        valFloat: Float,
                        valDouble: Double,
                        valTimestamp: java.sql.Timestamp,
                        valArray: Seq[Int],
                        valMap: Map[String, String],
                        valStruct: InternalStruct) {

  def this(key: Key, value: Value, topic: String, partition: Int, offset: Long) =
    this(value.valStr, key.keyStr, value.valInt, key.keyInt, topic, partition, offset,
      value.valByte, value.valShort, value.valLong, value.valFloat, value.valDouble,
      value.valTimestamp, value.valArray, value.valMap, value.valStruct)
}

object ResultVerify {
  def fields: Seq[String] = Seq("valStr", "keyStr", "valInt",
    "keyInt", "topic", "partition", "offset", "valByte", "valShort", "valLong", "valFloat",
    "valDouble", "valTimestamp", "valArray", "valMap", "valStruct")
}

abstract class BaseKafkaUnsafeRelationSuite extends QueryTest with SharedSQLContext with KafkaTest {

  import testImplicits._

  val testKeyValueMsgs = (0 to 9).map(i => (new Key(i).encode, new Value(i).encode))
  val testKeyMsgs = (0 to 9).map(i => new Key(i).encode)
  val testValueMsgs = (0 to 9).map(i => new Value(i).encode)
  val k = new Key(1)
  val v = new Value(1)

  val keySchema = k.schema
  val valueSchema = v.schema
  val fewKeyAnvValueFields = StructType(Array(k.schema.fields.head, v.schema.fields.head))

  val fixedFieldsKeyAndValueSchema = new StructType(
    KafkaUnsafeRelation.fixedSchema.fields ++ keySchema.fields ++ valueSchema.fields)

  val keyAndValueSchema = new StructType(
    k.schema.fields ++ v.schema.fields)

  private val topicId = new AtomicInteger(0)
  protected var testUtils: KafkaUnsafeTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaUnsafeTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  def sendMessage(topic: String, numPartition: Int,
                  batch: Int = 1): Map[Int, Seq[ResultVerify]] = {
    testUtils.withNewProducer { p =>
      val array = testKeyValueMsgs.toArray
      val seq = for (b <- (0 to batch - 1);
                     partition <- 0 to (numPartition - 1)) yield {
        testUtils.sendMessages(p, topic, partition, array)
        (partition, testKeyValueMsgs.zipWithIndex.map { case (x, index) =>
          new ResultVerify(new Key(index), new Value(index), topic, partition,
            (b * testKeyMsgs.size) + index.toLong)
        }.toSeq)
      }
      seq.toMap
    }
  }

  protected def newTopic(): String = s"${topicPrefix}-${topicId.getAndIncrement()}"

  protected def createDFWithAllFields(
                           topic: String,
                           partitions: Int,
                           withOptions: Map[String, String] = Map.empty[String, String],
                           brokerAddress: Option[String] = None) = {
    createDFWithFields(topic, partitions, fixedFieldsKeyAndValueSchema, withOptions, brokerAddress )
  }

  protected def createDFWithFields(
                          topic: String,
                          partitions: Int,
                          schema : StructType,
                          withOptions: Map[String, String] = Map.empty[String, String],
                          brokerAddress: Option[String] = None) = {
    val df = spark
      .read
      .format("org.apache.spark.sql.kafka010.KafkaUnsafeRelationProvider")
      .schema(schema)
      .option("kafka.bootstrap.servers", brokerAddress.getOrElse(testUtils.brokerAddress))
      .option("kafka.consumer.id", "test-consumer-id")
      .option("key-columns", keySchema.fields.map(_.name).mkString(","))
      .option("value-columns", valueSchema.fields.map(_.name).mkString(","))
      .option("topic", topic)

    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load()
  }

  protected def createDFWithFieldsAndNullColumns(
                                    topic: String,
                                    partitions: Int,
                                    schema : StructType,
                                    additionalKeyFields : StructType,
                                    additionValueFields : StructType,
                                    withOptions: Map[String, String] = Map.empty[String, String],
                                    brokerAddress: Option[String] = None) = {

    val tableSchema = StructType(schema.fields ++
      additionalKeyFields.fields ++
      additionValueFields.fields)

    val df = spark
      .read
      .format("org.apache.spark.sql.kafka010.KafkaUnsafeRelationProvider")
      .schema(tableSchema)
      .option("kafka.bootstrap.servers", brokerAddress.getOrElse(testUtils.brokerAddress))
      .option("kafka.consumer.id", "test-consumer-id")
      .option("key-columns",
        (keySchema.fields ++ additionalKeyFields.fields).map(_.name).mkString(","))
      .option("value-columns",
        (valueSchema.fields ++ additionValueFields).map(_.name).mkString(","))
      .option("topic", topic)

    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load()
  }

  protected def topicPrefix: String

  private def assignString(topic: String, partitions: Iterable[Int]): String = {
    JsonUtils.partitions(partitions.map(p => new TopicPartition(topic, p)))
  }
}

class KafkaUnsafeRelationSuite extends BaseKafkaUnsafeRelationSuite {

  import testImplicits._

  def topicPrefix: String = "unsafe-relation"

  test("Run simple sql with some rows") {
    testWithAllSchema
  }

  test("Run sql with only keyValue schema") {
    testWithSchema(keyAndValueSchema)
  }

  test("Run sql with only key schema") {
    testWithSchema(keySchema)
  }

  test("Run sql with only value schema") {
    testWithSchema(valueSchema)
  }

  test("Run sql with few key and value fields") {
    testWithSchema(fewKeyAnvValueFields)
  }

  test("Run sql with null key and value fields") {
    testWithAdditionNullColumns(keyAndValueSchema)
  }

  test("Performance with million rows") {
    val topic = newTopic()
    val partitions = 4
    val batches = 500
    testUtils.createTopic(topic, partitions = partitions)
    val df = createDFWithAllFields(topic, partitions,
      withOptions = Map())
    assert(df.count() === 0)
    val toVerify = sendMessage(topic, partitions, batches)
    assert(df.count() === (batches * partitions * testKeyValueMsgs.size))
    assert(
      df.selectExpr("count(distinct partition, offset)")
        .collect().head.getAs[Long](0) === (batches * partitions * testKeyValueMsgs.size))
  }

  test("Run sql with partition filters") {
    val topic = newTopic()
    val partitions = 2
    testUtils.createTopic(topic, partitions = partitions)
    val df = createDFWithAllFields(topic, partitions,
      withOptions = Map())
    val toVerify = sendMessage(topic, partitions)

    // test partition filters
    assert(df.filter("partition=1").count() === 10)
    assert(df.filter("partition=0").count() === 10)
    assert(df.filter("partition=3").count() === 0)
    assert(df.filter("partition in (0)").count() === 10)
    assert(df.filter("partition in (0,1)").count() === 20)
    assert(df.filter("partition > 0 ").count() === 10)
    assert(df.filter("partition < 1 ").count() === 10)
  }

  test("Run sql with offset_bounds filter") {
    val topic = newTopic()
    val partitions = 2
    testUtils.createTopic(topic, partitions = partitions)
    val df = createDFWithAllFields(topic, partitions,
      withOptions = Map())
    val toVerify = sendMessage(topic, partitions)

    // test bounds
    val df1 = df.filter("""offset_bounds(partition, offset, "0:1, 1:5", "0:8, 1:9" )""")
    val select1 = df1.select(ResultVerify.fields.head, ResultVerify.fields.tail: _*)
      .filter("partition = 0")
      .orderBy("offset")
    checkAnswer(select1,
      toVerify(0).filter(x => x.offset >= 1 && x.offset < 8).toDS().toDF())

    val select2 = df1.select(ResultVerify.fields.head, ResultVerify.fields.tail: _*)
      .filter("partition = 1")
      .orderBy("offset")
    checkAnswer(select2,
      toVerify(1).filter(x => x.offset >= 5 && x.offset < 9).toDS().toDF())

    // With multiple offset bounds
    val twoOffsetBounds = df.filter(
      """offset_bounds(partition, offset, "0:1, 1:5", "0:8, 1:9" )""")
      .filter("""offset_bounds(partition, offset, "0:1, 1:6", "0:8, 1:9" )""")
    assertThrows[RuntimeException](twoOffsetBounds.count())
  }

  test("Timestamp filter with time constraints") {
    val lowerBound = 123L
    val upperBound = 230L
    import org.apache.spark.sql.catalyst.dsl.expressions._
    val lowerBounds: Seq[Expression] = Seq('timestamp > lowerBound expr,
      'timestamp >= lowerBound expr,
      Literal(123) < 'timestamp, Literal(123) <= 'timestamp)

    lowerBounds.foreach { expr =>
      val res = KafkaUnsafeRelation.timeBounds(Seq(expr))
      assert(res === ((Some(lowerBound), None)))
    }

    val multiLowerBounds: Seq[Expression] = Seq('timestamp > lowerBound expr,
      'timestamp >= (lowerBound + 1) expr,
      Literal(lowerBound + 2) < 'timestamp, Literal(lowerBound + 3) <= 'timestamp)

    val mlRes = KafkaUnsafeRelation.timeBounds(multiLowerBounds)
    assert(mlRes === ((Some(lowerBound + 3), None)))

    val upperBounds: Seq[Expression] = Seq('timestamp < 230 expr, 'timestamp <= 230 expr,
      Literal(230) > 'timestamp, Literal(230) >= 'timestamp)

    upperBounds.foreach { expr =>
      val res = KafkaUnsafeRelation.timeBounds(Seq(expr))
      assert(res === ((None, Some(upperBound + 1))))
    }

    val multiUpperBound: Seq[Expression] = Seq('timestamp < upperBound expr,
      'timestamp <= (upperBound - 1) expr,
      Literal(upperBound - 2) > 'timestamp, Literal(upperBound - 3) >= 'timestamp)

    val muRes = KafkaUnsafeRelation.timeBounds(multiUpperBound)
    assert(muRes === ((None, Some(upperBound - 2))))

    val equalBounds: Seq[Expression] = Seq('timestamp === lowerBound expr,
      Literal(lowerBound) === 'timestamp)

    equalBounds.foreach { expr =>
      val res = KafkaUnsafeRelation.timeBounds(Seq(expr))
      assert(res === ((Some(lowerBound), Some(lowerBound + 1))))
    }

    val lowerAndUpperBounds: Seq[Seq[Expression]] =
      Seq(Seq('timestamp > lowerBound expr, 'timestamp < upperBound expr),
        Seq('timestamp >= lowerBound expr, 'timestamp <= upperBound expr),
        Seq('timestamp <= upperBound expr, Literal(lowerBound) < 'timestamp expr),
        Seq('timestamp <= upperBound expr, Literal(lowerBound) <= 'timestamp expr))

    lowerAndUpperBounds.foreach { x =>
      val resMulti = KafkaUnsafeRelation.timeBounds(x)
      assert(resMulti === ((Some(lowerBound), Some(upperBound + 1))))
    }

    val lowerAndUpperBoundsComplex: Seq[Expression] =
      Seq('timestamp > lowerBound or 'timestamp < upperBound expr,
        'timestamp >= lowerBound or 'timestamp <= upperBound expr,
        Or('timestamp <= upperBound expr, Literal(lowerBound) < 'timestamp),
        Or('timestamp <= upperBound expr, Literal(lowerBound) <= 'timestamp))

    lowerAndUpperBoundsComplex.foreach { x =>
      val resMulti = KafkaUnsafeRelation.timeBounds(Seq(x))
      assert(resMulti === ((None, None)))
    }

    val multiUpperLowerBound = multiUpperBound ++ multiLowerBounds
    val mluRes = KafkaUnsafeRelation.timeBounds(multiUpperLowerBound)
    assert(mluRes.===((Some(lowerBound + 3), Some(upperBound - 2))))
  }

  test("Unsafe Iterator should process excluding last offset ") {
    val topic = newTopic()
    val partitions = 1
    val bootstrapServers = new JArrayList[String]();
    bootstrapServers.add(testUtils.brokerAddress)
    testUtils.createTopic(topic, partitions = partitions)
    testUtils.sendMessages(topic, testKeyValueMsgs.toArray)
    val bufferSize = 1024 * 1024
    val numKeyFields = 2
    val config = new Properties()
    val map = new java.util.HashMap[String, Object]();

    val consumerClient = KafkaConsumerClientRegistry.INSTANCE.getOrCreate(bootstrapServers, config)
    for (maxOffset <- 2 to testKeyValueMsgs.size) {
      val sourcePartition = KafkaUnsafeSourceRDDPartition(0,
        KafkaUnsafeSourceRDDOffsetRange(new Node(consumerClient.leastLoadedNode()),
          Seq(new Node(consumerClient.leastLoadedNode())),
          new TopicPartition(topic, 0), 0, maxOffset, None))
      val iterator = new KafkaUnsafeIterator( sourcePartition,
        numKeyFields, valueSchema.size, bootstrapServers, map)
      var count = 0L;
      while (iterator.hasNext) {
        val next = iterator.next()
        count += 1
      }
      assert(count === maxOffset)
    }
  }

  test("Unsafe Iterator should process including start offset ") {
    val topic = newTopic()
    val partitions = 1
    val bootstrapServers = new JArrayList[String]();
    bootstrapServers.add(testUtils.brokerAddress)
    testUtils.createTopic(topic, partitions = partitions)
    testUtils.sendMessages(topic, testKeyValueMsgs.toArray)
    val bufferSize = 1024 * 1024
    val numKeyFields = 2
    val config = new Properties()
    val map = new java.util.HashMap[String, Object]();
    val consumerClient = KafkaConsumerClientRegistry.INSTANCE.getOrCreate(bootstrapServers, config)
    for (minOffset <- 0 to testKeyValueMsgs.size - 1) {
      val sourcePartition = KafkaUnsafeSourceRDDPartition(0,
        KafkaUnsafeSourceRDDOffsetRange(new Node(consumerClient.leastLoadedNode()),
          Seq(new Node(consumerClient.leastLoadedNode())), new TopicPartition(topic, 0), minOffset,
          testKeyValueMsgs.size, None))
      val iterator = new KafkaUnsafeIterator( sourcePartition, numKeyFields,
        valueSchema.size, bootstrapServers, map)
      var count = 0L;
      while (iterator.hasNext) {
        val next = iterator.next()
        count += 1
      }
      assert(count === testKeyValueMsgs.size - minOffset)
    }
  }

  test("Unsafe Iterator should process records ") {
    val topic = newTopic()
    val partitions = 1
    val bootstrapServers = new JArrayList[String]();
    bootstrapServers.add(testUtils.brokerAddress)
    testUtils.createTopic(topic, partitions = partitions)
    testUtils.sendMessages(topic, testKeyValueMsgs.toArray)

    val bufferSize = 1024 * 1024
    val numKeyFields = 2
    val config = new Properties()
    val map = new java.util.HashMap[String, Object]();
    val consumerClient = KafkaConsumerClientRegistry.INSTANCE.getOrCreate(bootstrapServers, config)

    val sourcePartition = KafkaUnsafeSourceRDDPartition(0,
      KafkaUnsafeSourceRDDOffsetRange(new Node(consumerClient.leastLoadedNode()),
        Seq(new Node(consumerClient.leastLoadedNode())),
        new TopicPartition(topic, 0), 0, testKeyValueMsgs.size, None))
    val iterator = new KafkaUnsafeIterator( sourcePartition, numKeyFields,
      valueSchema.size, bootstrapServers, map)
    while (iterator.hasNext) {
      val next = iterator.next()
      assert(next.getUTF8String(4).toString.startsWith("key"))
      assert(next.getInt(5) > -1)
      assert(next.getUTF8String(6).toString.startsWith("val"))
      assert(next.getInt(7) > -1)
    }

    val mappedIterator = new KafkaUnsafeIterator(
      sourcePartition, numKeyFields,
      valueSchema.size, bootstrapServers, map, Some((0 to 7).reverse.toArray))
    while (mappedIterator.hasNext) {
      val next = mappedIterator.next()
      assert(next.getUTF8String(3).toString.startsWith("key"))
      assert(next.getInt(2) > -1)
      assert(next.getUTF8String(1).toString.startsWith("val"))
      assert(next.getInt(0) > -1)
    }
  }

  test("KafkaUnsafe Fetcher should be able to get start offset") {
    val topic = newTopic()
    val numPartitions = 1
    val bootstrapServers = new JArrayList[String]();
    bootstrapServers.add(testUtils.brokerAddress)
    testUtils.createTopic(topic, numPartitions)
    testUtils.sendMessages(topic, testKeyValueMsgs.toArray)
    val startClientId = "offset-test"

    val buffer = ByteBuffer.allocate(4096)
    val tp = new TopicPartition(topic, 0)
    val lconnection = getTestClientConnection()

      val partitions = KafkaUnsafeFetcher.getPartitions(lconnection, topic)
      val earliest = KafkaUnsafeFetcher.earliestOffsets(lconnection, partitions,
        Seq(tp))
      assert(earliest == Map(tp -> ResponseOffset(0, -1)))

      val latest = KafkaUnsafeFetcher.latestOffsets(lconnection, partitions,
        Seq(tp))
      assert(latest == Map(tp -> ResponseOffset(10, -1)))
      val timestamp = System.currentTimeMillis()
      val sleepMills = 1000
      Thread.sleep(sleepMills)
      testUtils.sendMessages(topic, testKeyValueMsgs.toArray)

      val latest2 = KafkaUnsafeFetcher.latestOffsets(lconnection, partitions,
        Seq(tp))
      Thread.sleep(sleepMills)
      testUtils.sendMessages(topic, testKeyValueMsgs.toArray)
      val timestampOffsets = KafkaUnsafeFetcher.offsetsForTimestamp(lconnection, partitions,
        Map(tp -> timestamp))
      val offsets = timestampOffsets.map(e => e._1 -> e._2.offset)
      assert(offsets === Map(tp -> 10))
      assert(timestampOffsets(tp).timestamp >= timestamp)
  }

  private def testWithAllSchema(): Unit = {
    val topic = newTopic()
    val partitions = 2
    testUtils.createTopic(topic, partitions = partitions)
    val df = createDFWithAllFields(topic, partitions,
      withOptions = Map())
    assert(df.count() === 0)
    val toVerify = sendMessage(topic, partitions)
    val select = df.select(ResultVerify.fields.head, ResultVerify.fields.tail: _*)
      .orderBy("partition", "offset")
    checkAnswer(select, (toVerify(0) ++ toVerify(1)).toDS().toDF())
  }

  private def testWithSchema(schema : StructType): Unit = {
    val topic = newTopic()
    val partitions = 2
    testUtils.createTopic(topic, partitions = partitions)
    val df = createDFWithFields(topic, partitions, schema,
      withOptions = Map())
    assert(df.count() === 0)
    val toVerify = sendMessage(topic, partitions)
    val select = df.select(schema.fields.head.name, schema.fields.map(_.name): _*)
      .orderBy(schema.fields.head.name)
    val first = select.first()
    assert(first.get(0) != null )
    val count = select.count()
    assert((toVerify(0) ++ toVerify(1)).size === count )
  }

  private def testWithAdditionNullColumns(schema : StructType): Unit = {
    val topic = newTopic()
    val partitions = 2
    testUtils.createTopic(topic, partitions = partitions)
    val additionalKeyFields = (new StructType())
      .add("keyNullInt", IntegerType)
      .add("keyNullString", StringType)

    val additionValueFields = (new StructType())
      .add("valueNulInt", IntegerType)
      .add("valueNullString", StringType)
    val df = createDFWithFieldsAndNullColumns(topic, partitions, schema,
      additionalKeyFields, additionValueFields,
      withOptions = Map())
    assert(df.count() === 0)
    val toVerify = sendMessage(topic, partitions)
    val select = df.select(ResultVerify.fields.head, ResultVerify.fields.tail: _*)
      .orderBy("partition", "offset")
    checkAnswer(select, (toVerify(0) ++ toVerify(1)).toDS().toDF())
    val nullFields = additionalKeyFields.fields ++ additionValueFields.fields
    val allNull = df.select(nullFields.head.name, nullFields.tail.map(_.name) : _*)
    allNull.show(1000)
    allNull.collect().foreach { r =>
      for (i <- 0 to (r.length -1 )) {

        assert(r.isNullAt(i))
      }
    }
  }

  private def getTestClientConnection() = {
    val bootstrapServers = new JArrayList[String]();
    bootstrapServers.add(testUtils.brokerAddress)
    val properties = new Properties()
    properties.put("kafka.consumer.id", "test-consumer-id")
    KafkaConsumerClientRegistry.INSTANCE.getOrCreate(bootstrapServers, properties)
  }
}
