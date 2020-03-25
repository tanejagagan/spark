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

import java.sql.Timestamp
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

import org.apache.spark.sql.catalyst.json.UnsafeJsonEncoder
import org.apache.spark.sql.types._


trait UnsafeSerializable {
  def schema: StructType

  def toJson: String

  def encode: Array[Byte] = {
    encoder(toJson.getBytes())
  }

  def encoder: Array[Byte] => Array[Byte]
}

case class Key(keyStr: String, keyInt: Int) extends UnsafeSerializable {
  def this(i: Int) = this("key-" + i, i)

  import org.json4s.jackson.Serialization
  import org.json4s.NoTypeHints

  val schema = new StructType()
    .add("keyStr", StringType)
    .add("keyInt", IntegerType)

  val encoder = UnsafeJsonEncoder.encoderFor(schema.toDDL)
  private implicit val formats = Serialization.formats(NoTypeHints)

  import Serialization.write

  def toJson: String = write(this)
}

case class InternalStruct(inner1: String, inner2: String)

case class Value(valStr: String, valInt: Int,
                 valByte: Byte,
                 valShort: Short,
                 valLong: Long,
                 valFloat: Float,
                 valDouble: Double,
                 valTimestamp: java.sql.Timestamp,
                 valArray: Seq[Int],
                 valMap: Map[String, String],
                 valStruct: InternalStruct,
                 additional: Option[String]) extends UnsafeSerializable {
  def this(i: Int) = this("value-" + i, i, i.toByte, i.toShort, i.toLong, i.toFloat,
    i.toDouble, new Timestamp(i), Array(i), Map(i.toString -> i.toString),
    new InternalStruct(i.toString, i.toString),
    None)

  def this(i: Int, additional: String) =
    this("value-" + i, i, i.toByte, i.toShort, i.toLong, i.toFloat,
      i.toDouble, new Timestamp(i), Array(i), Map(i.toString -> i.toString),
      new InternalStruct(i.toString, i.toString), Some(additional))

  import org.json4s.jackson.Serialization
  import org.json4s.NoTypeHints

  private implicit val formats = Serialization.formats(NoTypeHints)

  import Serialization.write

  val internalStructSchema = new StructType()
    .add("inner1", StringType)
    .add("inner2", StringType)

  val schema = new StructType()
    .add("valStr", StringType)
    .add("valInt", IntegerType)
    .add("valByte", ByteType)
    .add("valShort", ShortType)
    .add("valLong", LongType)
    .add("valFloat", FloatType)
    .add("valDouble", DoubleType)
    .add("valTimestamp", TimestampType)
    .add("valArray", ArrayType(IntegerType))
    .add("valMap", MapType(StringType, StringType))
    .add("valStruct", internalStructSchema)

  val encoderForLessFields = UnsafeJsonEncoder.encoderFor(
    new StructType().add("valStr", StringType).toDDL)

  val encoderForMoreFields = UnsafeJsonEncoder.encoderFor(
    StructType(schema.fields).add("additional", StringType).toDDL)

  val encoder = UnsafeJsonEncoder.encoderFor(schema.toDDL)

  def toJson: String = write(this)
}

class KafkaUnsafeTestUtils(withBrokerProps: Map[String, Object] = Map.empty)
  extends KafkaTestUtils(withBrokerProps) {


  def sendJsonMessages(keySchema: String, valueSchema: String,
                       topic: String, keyValueJson: Array[(String, String)]): Unit = {
    val keyEncoder = UnsafeJsonEncoder.encoderFor(keySchema)
    val valueEncoder = UnsafeJsonEncoder.encoderFor(valueSchema)
    val keyValues = keyValueJson.map(x =>
      (keyEncoder(x._1.getBytes()), valueEncoder(x._2.getBytes())))
    sendMessages(topic, keyValues)
  }

  def sendMessages(topic: String,
                   keyValues: Seq[(Array[Byte], Array[Byte])]) {
    val producer: Producer[Array[Byte], Array[Byte]] =
      new KafkaProducer[Array[Byte], Array[Byte]](producerConfiguration)
    // producer.beginTransaction()
    keyValues.foreach { x =>
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, x._1, x._2))
    }
    producer.close()
    // producer.commitTransaction()
  }

  def sendMessages( producer : KafkaProducer[Array[Byte], Array[Byte]], topic : String,
                   keyValues: Seq[(Array[Byte], Array[Byte])]): Unit = {
    keyValues.foreach { x =>
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, x._1, x._2))
    }
  }

  def withNewProducer[T]( op : KafkaProducer[Array[Byte], Array[Byte]] => T) : T = {
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfiguration)
      val res = op(producer)
      producer.close()
    res
  }

  def withNewTxnProducer[T] ( op : KafkaProducer[Array[Byte], Array[Byte]] => T) : T = {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfiguration)
    val res = op(producer)
    producer.close()
    res
  }

  def sendMessages(producer : KafkaProducer[Array[Byte], Array[Byte]],
    topic: String, partition: Int, keyValues: Seq[(Array[Byte], Array[Byte])]) {
      keyValues.foreach { x =>
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic,
          partition, x._1, x._2))
    }
  }

  def sendMessages(topic: String, partition: Int, keyValues: Seq[(Array[Byte], Array[Byte])]) {
    withNewProducer { producer =>
      // producer.beginTransaction()
      keyValues.foreach { x =>
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic,
          partition, x._1, x._2))
      }
    }
    // producer.commitTransaction()
  }

  def sendKeyMessage(topic: String, keys: Seq[Array[Byte]]): Unit = {
    val keyValues: Seq[(Array[Byte], Array[Byte])] = keys.map(k =>
      (k, null.asInstanceOf[Array[Byte]])
    )
    sendMessages(topic, keyValues)
  }

  def sendValueMessage(topic: String, values: Seq[Array[Byte]]): Unit = {
    val keyValues: Seq[(Array[Byte], Array[Byte])] = values.map(v =>
      (null.asInstanceOf[Array[Byte]], v)
    )
    sendMessages(topic, keyValues)
  }

  private def producerConfiguration: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerAddress)
    props.put("value.serializer", classOf[ByteArraySerializer].getName)
    props.put("key.serializer", classOf[ByteArraySerializer].getName)
    // wait for all in-sync replicas to ack sends
    props.put("acks", "all")
    // props.put("transactional.id", "transactional-id-" + System.currentTimeMillis())
    props
  }
}
