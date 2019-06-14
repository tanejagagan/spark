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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{FileStreamSinkLog, OffsetSeqLog}
import org.apache.spark.sql.kafka010.KafkaSourceProvider.STARTING_OFFSETS_OPTION_KEY
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}


class KafkaDeltaRelationProvider extends RelationProvider with Logging {
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val startingOffsetJson = KafkaDeltaRelationProvider.getStartingOffsets(sqlContext, parameters)
    val sp = new KafkaSourceProvider()
    sp.createRelation(sqlContext, parameters. + (STARTING_OFFSETS_OPTION_KEY -> startingOffsetJson))
  }
}

object KafkaDeltaRelationProvider {
  def getStartingOffsets(sqlContext: SQLContext, parameters: Map[String, String]) : String = {
    val checkpointLocation = parameters("checkpointLocation")
    val sinkLocation = parameters("sinkMetadataLocation")
    val sink = new FileStreamSinkLog(FileStreamSinkLog.VERSION,
      sqlContext.sparkSession, appendSlash(sinkLocation) + "_spark_metadata")
    val oLatestBatch = sink.getLatest().map(_._1)
    assert(oLatestBatch.isDefined, "No metadata found at the sink location")
    val latestBatch = oLatestBatch.get
    val offsetLog = new OffsetSeqLog(sqlContext.sparkSession,
      appendSlash(checkpointLocation) + "offsets")

    val offsetSeq = offsetLog.get(latestBatch).get
    offsetSeq.offsets.head.get.json()

  }

  private def appendSlash(path : String): String = {
    if (path.endsWith("/")) {
      path
    } else {
      path + "/"
    }
  }
}
