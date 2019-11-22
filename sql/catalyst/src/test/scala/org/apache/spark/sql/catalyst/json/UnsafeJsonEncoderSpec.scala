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

package org.apache.spark.sql.catalyst.json

import org.scalatest.WordSpec

import org.apache.spark.sql.catalyst.expressions.UnsafeRow


class UnsafeJsonEncoderSpec extends WordSpec {
  "EncoderBuilder" should {
    "encode json correctly" in {
      val schemaStr = "intVal int, stringVal string"
      val json = """{"intVal": 10, "stringVal": "10"}"""
      val encoder = UnsafeJsonEncoder.builder(schemaStr)
      val encoder2 = UnsafeJsonEncoder.builder(schemaStr + ", floatVal float")

      val start = System.currentTimeMillis()
      var i = 0;
      while (i < 100000) {
        val byteArray = encoder(json.getBytes())
        val unsafeRow = UnsafeRow.readExternal(byteArray)
        i+=1
      }
      val end = System.currentTimeMillis()
    }
  }
}
