package com.day30

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object StreamingCapstoneApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day30-Final-E-Commerce-Streaming")
      .setMaster("local[*]")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.sparkContext.setLogLevel("WARN")

    val spark = SparkSession.builder()
      .config(streamingContext.sparkContext.getConf)
      .getOrCreate()

    val invalidRecords =
      streamingContext.sparkContext.longAccumulator("Invalid Streaming Records")

    val customerBroadcast =
      streamingContext.sparkContext.broadcast(
        Map(
          "C001" -> "Aarav",
          "C002" -> "Ananya",
          "C003" -> "Rahul",
          "C004" -> "Sneha",
          "C005" -> "Vikram",
          "C006" -> "Meera"
        )
      )

    val stream =
      streamingContext
        .textFileStream("data/stream")
        .map(_.trim)
        .filter(_.nonEmpty)

    val parsedOrders = stream.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 6) {
        try {
          val orderId = parts(0)
          val customerId = parts(1)
          val productId = parts(2)
          val quantity = parts(3).toInt
          val amount = parts(4).toDouble
          val status = parts(5)

          Some(
            (
              orderId,
              customerId,
              productId,
              quantity,
              amount,
              status
            )
          )

        } catch {
          case _: Exception =>
            invalidRecords.add(1)
            None
        }

      } else {
        invalidRecords.add(1)
        None
      }
    }

    val completedOrders =
      parsedOrders
        .filter(_._6 == "COMPLETED")
        .cache()

    val customerRevenue =
      completedOrders
        .map {
          case (_, customerId, _, _, amount, _) =>
            val customerName =
              customerBroadcast.value.getOrElse(customerId, "UNKNOWN")

            (customerId, (customerName, amount))
        }
        .reduceByKey {
          case ((name, amount1), (_, amount2)) =>
            (name, amount1 + amount2)
        }

    customerRevenue.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== STREAMING CUSTOMER REVENUE ==========")

        rdd
          .sortBy(_._2._2, ascending = false)
          .collect()
          .foreach {
            case (customerId, (customerName, revenue)) =>
              println(
                f"$customerId%-5s $customerName%-10s Revenue: $$${revenue}%.2f"
              )
          }

        println(
          s"Invalid streaming records: ${invalidRecords.value}"
        )
      }
    }

    val orderCountByStatus =
      parsedOrders
        .map {
          case (_, _, _, _, _, status) =>
            (status, 1L)
        }
        .reduceByKey(_ + _)

    orderCountByStatus.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println("\n========== STREAMING ORDER STATUS ==========")

        rdd
          .collect()
          .sortBy(_._1)
          .foreach {
            case (status, count) =>
              println(s"$status -> $count")
          }
      }
    }

    streamingContext.start()

    println("\n==============================================")
    println("Day 30 Streaming Capstone Started")
    println("Batch interval: 5 seconds")
    println("Watching: data/stream")
    println("Add CSV files to data/stream to generate events.")
    println("Press Ctrl+C to stop.")
    println("==============================================\n")

    streamingContext.awaitTermination()
  }
}
