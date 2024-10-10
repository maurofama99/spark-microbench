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
package org.apache.spark.examples.sql.streaming

// scalastyle:off println
import java.sql.Timestamp

import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.debug.DebugStreamQuery
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils


object StructuredSessionization {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredSessionization <hostname> <port>")
      System.exit(1)
    }
    val mode = "SESSION_1"


      /**
       * SESSION WINDOWS
       */

    if (mode == "SESSION_1") {

      val filepath = "/home/maurofama/spark-microbench/examples/src/main/" +
        "scala/org/apache/spark/examples/sql/streaming/files/csv_single" +
        "/sample-1000000-50ooo.csv"

      val parquetOutputPath = "/home/maurofama/spark-microbench/examples/src/main/" +
        "scala/org/apache/spark/examples/sql/streaming/files/csv_single" +
        "/sample-1000000-50ooo.parquet"

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

      val spark = SparkSession
        .builder()
        .appName("StructuredSessionization")
        .config("spark.master", "local")
        .config("spark.default.parallelism", 1)
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      val staticDataFrame = spark.read.load(parquetOutputPath)

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

      // Read the CSV file
      val csvDF = spark.readStream
        .schema(staticDataFrame.schema)
        .format("csv")
        .option("header", "true") // Se il file CSV ha una riga di intestazione
        .option("path", "/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/" +
          "spark/examples/sql/streaming/files/csv_single")
        .load()
        // .withColumn("timestamp", current_timestamp()) // create timestamp
        // .withColumn("ts", getRandomTimestampUDF())
      // Genera timestamp casuale per ogni riga

//      val userSchema = new StructType()
//        .add("ts", "timestamp")
//        .add("key", "integer")
//        .add("value", "integer")
//
//      val csvDF = spark
//        .readStream
//        .option("sep", ",")
//        .option("maxFilesPerTrigger", 10)
//        .schema(userSchema)      // Specify schema of the csv files
//        .csv("/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/" +
//          "spark/examples/sql/streaming/files/debug")

      // val rows = csvDF.select("ts", "key")

      val sessionDuration = s"7 seconds"

      val windowedCsvDF = csvDF
        // .withWatermark("ts", "4 seconds")
        .groupBy(
          session_window(col("ts"), sessionDuration),
          col("key")
        )
        .agg(collect_list(col("key")).as("elements"))
//
//      val windowDuration = s"10 seconds"
//      val slideDuration = s"1 seconds"
//
//      val windowedCsvDF = csvDF
//        .withWatermark("ts", "30 seconds")
//        .groupBy(
//          window(col("ts"), windowDuration, slideDuration)
//        )
//        .agg(collect_list(col("ts")).as("elements"))

      // windowedCsvDF.explain()

      // Start running the query that prints the sessioned word counts to the console
      val query = windowedCsvDF.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 400)
        .start()

      // query.debugCodegen()

      import scala.concurrent._
      import scala.concurrent.duration._

      // Function to check the status of the query and stop if conditions are met
      def stopQueryGracefully(query: StreamingQuery): Unit = {
        val stopCheck = Future {
          while (query.isActive) {
            val status = query.status
            val dataAvail = status.isDataAvailable
            val triggerActive = status.isTriggerActive
            val msg = status.message

            if (!dataAvail && !triggerActive && !Seq("Initializing " +
              "sources", "Initializing StreamExecution").contains(msg)) {
              println("Stopping query...")
              query.stop()
            }

            println(query.lastProgress)
            Thread.sleep(500)
          }
        }

        // Await termination for a given wait time
        println("Awaiting termination...")
        ThreadUtils.awaitResult(stopCheck, Duration.Inf)
        query.awaitTermination(60000) // Replace 60000 with desired wait time in milliseconds
      }

      // Call the function to stop the query gracefully
      // stopQueryGracefully(query)

            while(query.isActive) {
              println(query.lastProgress)
              Thread.sleep(10000)
            }

            query.awaitTermination()













    } else if (mode == "SESSION_2") {

      val spark = SparkSession
        .builder()
        .appName("StructuredSessionization")
        .config("spark.master", "local")
        .config("spark.default.parallelism", 1)
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      val userSchema = new StructType()
        .add("ts", "timestamp")
        .add("key", "integer")
        .add("value", "integer")

      val csvDF = spark
        .readStream
        .option("sep", ",")
        .option("maxFilesPerTrigger", 10)
        .schema(userSchema)      // Specify schema of the csv files
        .csv("/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/" +
          "spark/examples/sql/streaming/files/debug")

      val sessionDuration = s"7 seconds"

      val windowedCsvDF = csvDF
        .withWatermark("ts", "5 seconds")
        .groupBy(
          session_window(col("ts"), sessionDuration), col("key")
        )
        .agg(collect_list(col("ts")).as("elements"))

      // windowedCsvDF.explain()

      // Start running the query that prints the sessioned word counts to the console
      val query = windowedCsvDF.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 400)
        .start()

      query.debugCodegen()

      import scala.concurrent._
      import scala.concurrent.duration._

      // Function to check the status of the query and stop if conditions are met
      def stopQueryGracefully(query: StreamingQuery): Unit = {
        val stopCheck = Future {
          while (query.isActive) {
            val status = query.status
            val dataAvail = status.isDataAvailable
            val triggerActive = status.isTriggerActive
            val msg = status.message

            if (!dataAvail && !triggerActive && !Seq("Initializing " +
              "sources", "Initializing StreamExecution").contains(msg)) {
              println("Stopping query...")
              query.stop()
            }

            println(query.lastProgress)
            Thread.sleep(500)
          }
        }

        // Await termination for a given wait time
        println("Awaiting termination...")
        ThreadUtils.awaitResult(stopCheck, Duration.Inf)
        query.awaitTermination(60000) // Replace 60000 with desired wait time in milliseconds
      }

      // Call the function to stop the query gracefully
      stopQueryGracefully(query)

    } else {


















      /**
       * SLIDING WINDOWS
       */

      val spark = SparkSession
        .builder()
        .appName("StructuredSessionization")
        .config("spark.master", "local")
        .config("spark.default.parallelism", 1)
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      val userSchema = new StructType()
        .add("ts", "timestamp")
        .add("key", "integer")
        .add("value", "integer")

      val csvDF = spark
        .readStream
        .option("sep", ",")
        .option("maxFilesPerTrigger", 10)
        .schema(userSchema)      // Specify schema of the csv files
        .csv("/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/" +
          "spark/examples/sql/streaming/files/csv")

      val windowDuration = s"10 seconds"
      val slideDuration = s"1 seconds"

      val windowedCsvDF = csvDF
        .withWatermark("ts", "30 seconds")
        .groupBy(
          window(col("ts"), windowDuration, slideDuration)
        )
        .agg(collect_list(col("ts")).as("elements"))

      windowedCsvDF.explain()

      // Start running the query that prints the sessioned word counts to the console
      val query = windowedCsvDF.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 400)
        .start()

      query.debugCodegen()

       import scala.concurrent._
       import scala.concurrent.duration._

      // Function to check the status of the query and stop if conditions are met
      def stopQueryGracefully(query: StreamingQuery): Unit = {
        val stopCheck = Future {
          while (query.isActive) {
            val status = query.status
            val dataAvail = status.isDataAvailable
            val triggerActive = status.isTriggerActive
            val msg = status.message

            if (!dataAvail && !triggerActive && !Seq("Initializing " +
              "sources", "Initializing StreamExecution").contains(msg)) {
              println("Stopping query...")
              query.stop()
            }

               println(query.lastProgress)
            Thread.sleep(500)
          }
        }

        // Await termination for a given wait time
        println("Awaiting termination...")
        ThreadUtils.awaitResult(stopCheck, Duration.Inf)
        query.awaitTermination(60000) // Replace 60000 with desired wait time in milliseconds
      }

      // Call the function to stop the query gracefully
      stopQueryGracefully(query)

//      while(query.isActive) {
//        println(query.lastProgress)
//        Thread.sleep(10000)
//      }
//
//      query.awaitTermination()


    }

  }
}
// scalastyle:on println
