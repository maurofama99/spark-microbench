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

// scalastyle:off println
package org.apache.spark.examples.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, session_window, udf}

import java.sql.Timestamp


/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 *
 * Usage: StructuredSessionization <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.StructuredSessionization
 * localhost 9999`
 */
object StructuredSessionization {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredSessionization <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val filepath = "/home/maurofama/spark-microbench/examples/src/main/" +
      "scala/org/apache/spark/examples/sql/streaming/files/debug" +
      "/sample-200.csv"

    val parquetOutputPath = "/home/maurofama/spark-microbench/examples/src/main/" +
      "scala/org/apache/spark/examples/sql/streaming/files/debug" +
      "/sample-200.parquet"

    val sparkConv = SparkSession
      .builder()
      .appName("CSV to Parquet Conversion")
      .config("spark.master", "local")
      .getOrCreate()

    val csvData = sparkConv.read.format("csv")
      .option("header", "true")
      .load(filepath)

    csvData.write
      .mode("overwrite") // Sovrascrive se il file esiste già
      .parquet(parquetOutputPath)

    sparkConv.stop()


    val startTimestampSeconds = 1640995200L // Esempio: 1 gennaio 2022, 00:00:00 in formato UNIX
    var lastGeneratedTimestamp = startTimestampSeconds

    // Genera un timestamp casuale all'interno del range specificato per ogni riga
    val getRandomTimestampUDF = udf(() => {
      val minGap = 3 // Minimo intervallo aggiuntivo in secondi
      val maxGap = 10 // Massimo intervallo aggiuntivo in secondi

      val randomGap = minGap + (scala.util.Random.nextDouble() * (maxGap - minGap)).toLong
      val nextTimestamp = lastGeneratedTimestamp + randomGap

      lastGeneratedTimestamp = nextTimestamp

      new Timestamp(lastGeneratedTimestamp * 1000) // Converti in millisecondi
    })

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("StructuredSessionization")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
//    val lines = spark.readStream
//      .format("socket")
//      .option("host", host)
//      .option("port", port)
//      .option("includeTimestamp", true)
//      .load()

    // Split the lines into words, retaining timestamps
    // split() splits each line into an array, and explode() turns the array into multiple rows
    // treat words as sessionId of events
//    val events = lines
//      .selectExpr("explode(split(value, ' ')) AS sessionId", "timestamp AS eventTime")

    // Sessionize the events. Track number of events, start and end timestamps of session,
    // and report session updates.
//    val sessionUpdates = events
//      .groupBy(session_window($"eventTime", "10 seconds") as Symbol("session"), $"sessionId")
//      .agg(count("*").as("numEvents"))
//      .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
//        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
//        "numEvents")

    val staticDataFrame = spark.read.load(parquetOutputPath)

    val csvDF = spark.readStream
      .schema(staticDataFrame.schema)
      .format("csv")
      .option("header", "true") // Se il file CSV ha una riga di intestazione
      .option("path", "/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/" +
        "spark/examples/sql/streaming/files/debug")
      .load()
      // .withColumn("ts", getRandomTimestampUDF())

    val lines = csvDF.select($"ts", $"key")

    val sessionDuration = s"7 seconds"

    val windowedCsvDF = lines
      // .withWatermark("ts", "4 seconds")
      .groupBy(
        session_window(col("ts"), sessionDuration),
        col("key")
      )
      .agg(collect_list(col("key")).as("elements"))

    // Start running the query that prints the session updates to the console
    val query = windowedCsvDF
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
