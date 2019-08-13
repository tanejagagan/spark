package org.apache.spark.sql.execution.streaming

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class NGFileStreamSinkLogSuite extends SparkFunSuite with SharedSQLContext {

  test("deserialize with no partition schema") {
    val parent = "/a/b/x"
    val dataPath = parent
    val partitionSchema = StructType(Seq())
    test(parent, dataPath, partitionSchema)
  }

  test("deserialize with int partition schema") {
    val partitionSchema = StructType(Seq(StructField("zipcode",
      IntegerType, nullable = false)))
    val parent = "/a/b/x/zipcode=123"
    val dataBasePath = "/a/b/x"
    test(parent, dataBasePath, partitionSchema)
  }

  test("deserialize with array partition schema") {

    val parent = new Path("/a/b/x/range=[0,10]").toUri.toString
    val dataBasePath = "/a/b/x"
    val partitionSchema = StructType(Seq(StructField("range",
      ArrayType(IntegerType), nullable = false)))
    test(parent, dataBasePath, partitionSchema)
  }

  test("deserialize with String  partition schema") {
    val parent = new Path("/a/b/x/range=0_10").toUri.toString
    val dataBasePath = "/a/b/x"
    val partitionSchema = StructType(Seq(StructField("range",
      StringType, nullable = false)))
    test(parent, dataBasePath, partitionSchema)
  }

  private def test(parent: String, dataBasePath: String, partitionSchema: StructType): Unit = {
    withFileStreamSinkLog(partitionSchema, dataBasePath) { log =>
      val latestBatchID = log.getLatestBatchId()
      assert(latestBatchID === None)
      val zero = newTestBatch(parent, 0)
      log.add(0, zero)
      val zeroFromLog = log.get(0)
      assert(zeroFromLog.map(_.sortBy(_.path).toSeq) ===
        Some(zero.sortBy(_.path).toSeq))

      assert(log.allFiles().size === 3)
      assert(log.sizeInBytes === 600)
      assert(log.inputFiles.sorted === zero.map(_.path).sorted)

      val one = newTestBatch(parent, 1)
      log.add(1, one)
      val oneFromLog = log.get(1)
      assert(oneFromLog.map(_.sortBy(_.path).toSeq) ===
        Some(one.sortBy(_.path).toSeq))

      val oneAndOne = log.get(Some(1), Some(1))
      assert(oneAndOne.head._2.sortBy(_.path) === one.sortBy(_.path))

      val oneAndTwo = log.get(Some(0), None).sortBy(_._1).map(_._2.sortBy(_.path))

      assert(oneAndTwo ===
        Array(zero.sortBy(_.path)) ++ Seq(one.sortBy(_.path)))

      val files = log.listFiles(Seq())
    }
  }

  private def withFileStreamSinkLog(partitionSchema: StructType,
                                    dataBasePath: String)(f: NGFileStreamSinkLog => Unit) {
    withTempDir { file =>
      val metadata = new File(file, "_spark_metadata")
      metadata.mkdir()
      val sinkLog = new NGFileStreamSinkLog(
        FileStreamSinkLog.VERSION, spark,
        metadata.getCanonicalPath, dataBasePath, Some(partitionSchema), "parquet")
      f(sinkLog)
    }
  }

  private def newTestBatch(parent: String, batchId: Long) = {
    val currentTimeMillis = System.currentTimeMillis()
    Array(
      SinkFileStatus(
        batchId = batchId,
        path = s"$parent/${batchId + "_" + currentTimeMillis}",
        size = 100L,
        isDir = false,
        modificationTime = 1000L,
        blockReplication = 1,
        blockSize = 10000L,
        action = FileStreamSinkLog.ADD_ACTION),
      SinkFileStatus(
        batchId = batchId,
        path = s"$parent/${batchId + "_" + currentTimeMillis + "_"}",
        size = 200L,
        isDir = false,
        modificationTime = 2000L,
        blockReplication = 2,
        blockSize = 20000L,
        action = FileStreamSinkLog.DELETE_ACTION),
      SinkFileStatus(
        batchId = batchId,
        path = s"$parent/${batchId + "_" + currentTimeMillis + "__"}",
        size = 300L,
        isDir = false,
        modificationTime = 3000L,
        blockReplication = 3,
        blockSize = 30000L,
        action = FileStreamSinkLog.ADD_ACTION))
  }
}
