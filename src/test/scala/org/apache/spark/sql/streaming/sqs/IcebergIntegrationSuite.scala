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
package org.apache.spark.sql.streaming.sqs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeUnit

class IcebergIntegrationSuite extends AnyFunSuite {

  val spark = SparkSession
    .builder()
    .appName("IntegrationTest")
    .master("local")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")

    .config("spark.sql.catalog.spark_catalog.warehouse", "lakehouse_dir")
    .config("spark.sql.warehouse.dir", "lakehouse_dir")

    .config("spark.sql.legacy.createHiveTableByDefault", "false")
    .config("spark.sql.sources.default", "iceberg")
    .config("spark.hadoop.fs.s3a.assumed.role.arn", "roleArn")
    .getOrCreate()

  test("iceberg integration test") {

    val segmentSchema = StructType(Array(
      StructField("channel", StringType, nullable = true),
      StructField("event", StringType, nullable = true),
      StructField("messageId", StringType, nullable = true),
      StructField("projectId", StringType, nullable = true),
      StructField("receivedAt", StringType, nullable = true)
    ))

    spark.sql("drop table test")
    val df = spark.sql("CREATE TABLE IF NOT EXISTS test(channel string, event string, messageId string, projectId string, receivedAt string)")
    df.printSchema()

    val reader = spark
      .readStream
      .format("s3-sqs")
      .option("fileFormat", "json")
      .option("sqsUrl", "sqsUrl")
      .option("maxFilesPerTrigger", "1")
      .option("sqsFetchIntervalSeconds", "2")
      .option("sqsLongPollingWaitTimeSeconds", "5")
      .option("ignoreFileDeletion", "true")
      .option("region", "eu-central-1")
      .schema(segmentSchema)
      .load()

    reader.printSchema()

    val query = reader.writeStream
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
      .option("path", "test")
      .option("checkpointLocation", "checkpointPath")
      .start()

    query.awaitTermination(timeoutMs = 10000)


    spark.sql("select * from test").show()
  }
}

