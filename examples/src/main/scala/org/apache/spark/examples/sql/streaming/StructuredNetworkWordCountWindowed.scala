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
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import scala.util.Random


/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network over a
 * sliding window of configurable duration. Each line from the network is tagged
 * with a timestamp that is used to determine the windows into which it falls.
 *
 * Usage: StructuredNetworkWordCountWindowed <hostname> <port> <window duration>
 *   [<slide duration>]
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 * <window duration> gives the size of window, specified as integer number of seconds
 * <slide duration> gives the amount of time successive windows are offset from one another,
 * given in the same units as above. <slide duration> should be less than or equal to
 * <window duration>. If the two are equal, successive windows have no overlap. If
 * <slide duration> is not provided, it defaults to <window duration>.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example sql.streaming.StructuredNetworkWordCountWindowed
 *    localhost 9999 <window duration in seconds> [<slide duration in seconds>]`
 *
 * One recommended <window duration>, <slide duration> pair is 10, 5
 */
object StructuredNetworkWordCountWindowed {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
        " <window duration in seconds> [<slide duration in seconds>]")
      System.exit(1)
    }

    val path = "/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/" +
      "spark/examples/sql/streaming/files/"
    val filepath = "/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/" +
      "spark/examples/sql/streaming/files/csv/sample-aggregate-1000-5.csv"
    val parquetOutputPath = "/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/" +
      "spark/examples/sql/streaming/files/csv/sample-aggregate-1000-5.parquet"


    val sparkConv = SparkSession
      .builder()
      .appName("CSV to Parquet Conversion")
      .config("spark.master", "local")
      .getOrCreate()

    val csvData = sparkConv.read.format("csv")
      .option("header", "true")
      .load(filepath)

    // Scrivi i dati convertiti nel formato Parquet
    csvData.write
      .mode("overwrite") // Sovrascrive se il file esiste già
      .parquet(parquetOutputPath)

    sparkConv.stop()

    val host = args(0)
    val port = args(1).toInt
    val windowSize = args(2).toInt
    val slideSize = if (args.length == 3) windowSize else args(3).toInt
    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val spark = SparkSession
      .builder()
      .appName("StructuredNetworkWordCountWindowed")
      .config("spark.master", "local")
      .config("spark.default.parallelism", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    /* val lines = spark.readStream
      .format("text")
      .option("path", path)
      .load() */

    val staticDataFrame = spark.read.load(path + "csv/sample-aggregate-1000-5.parquet")

    val startTimestampSeconds = 1640995200 // Esempio: 1 gennaio 2022, 00:00:00 in formato UNIX
    val endTimestampSeconds = 1640998800 // Esempio: 1 gennaio 2022, 01:00:00 in formato UNIX

    // Genera un timestamp casuale all'interno del range specificato per ogni riga
    val getRandomTimestampUDF = udf(() => {
      val randomTS = startTimestampSeconds + (Random.nextDouble() *
        (endTimestampSeconds - startTimestampSeconds)).toLong
      new Timestamp(randomTS * 1000) // Converti in millisecondi
    })

    val lines = spark.readStream
      .schema(staticDataFrame.schema)
      .format("csv")
      .option("header", "true") // Se il file CSV ha una riga di intestazione
      .option("path", path + "csv") // Inserisci il percorso del file CSV
      .load()
      // .withColumn("timestamp", current_timestamp()) // create timestamp
      .withColumn("timestamp", getRandomTimestampUDF()) // Genera timestamp casuale per ogni riga

    val words = lines.select($"timestamp", $"value".alias("word"))

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count()

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
