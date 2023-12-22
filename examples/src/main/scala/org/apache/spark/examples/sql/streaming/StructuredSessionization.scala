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

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.session_window

// import org.apache.spark.sql.functions.{count, session_window}


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

    val spark = SparkSession
      .builder()
      .appName("StructuredSessionization")
      .config("spark.master", "local")
      .config("spark.default.parallelism", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // Sessionize the events. Track number of events, start and end timestamps of session,
    // and report session updates.
    val sessionUpdates = words
      .groupBy(session_window($"timestamp", "10 seconds"), $"word")
      .count()// .explain()

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
