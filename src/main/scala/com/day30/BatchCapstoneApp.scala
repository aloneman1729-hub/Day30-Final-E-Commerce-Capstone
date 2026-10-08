package com.day30

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.util.LongAccumulator

object BatchCapstoneApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day30-Final-E-Commerce-Batch")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ------------------------------------------------------------
    // 1. Read source data
    // ------------------------------------------------------------

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/raw/customers.csv")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/raw/products.csv")

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/raw/orders.csv")

    println("\n========== SOURCE COUNTS ==========")
    println(s"Customers: ${customers.count()}")
    println(s"Products : ${products.count()}")
    println(s"Orders   : ${orders.count()}")

    // ------------------------------------------------------------
    // 2. Accumulator for cancelled orders
    // ------------------------------------------------------------

    val cancelledOrders: LongAccumulator =
      spark.sparkContext.longAccumulator("Cancelled Orders")

    orders
      .filter(col("status") === "CANCELLED")
      .foreach(_ => cancelledOrders.add(1))

    // ------------------------------------------------------------
    // 3. Filter valid completed orders
    // ------------------------------------------------------------

    val completedOrders = orders
      .filter(col("status") === "COMPLETED")
      .cache()

    println("\n========== DATA QUALITY ==========")
    println(s"Cancelled orders: ${cancelledOrders.value}")
    println(s"Completed orders: ${completedOrders.count()}")

    // ------------------------------------------------------------
    // 4. Partition tuning
    // ------------------------------------------------------------

    val partitionedOrders = completedOrders
      .repartition(4, col("customer_id"))

    println("\n========== PARTITIONING ==========")
    println(s"Partitions after repartition: ${partitionedOrders.rdd.getNumPartitions}")

    // ------------------------------------------------------------
    // 5. Broadcast join with products
    // ------------------------------------------------------------

    val enrichedOrders = partitionedOrders
      .join(
        broadcast(products),
        partitionedOrders("product_id") === products("product_id"),
        "inner"
      )
      .select(
        partitionedOrders("order_id"),
        partitionedOrders("customer_id"),
        partitionedOrders("product_id"),
        products("product_name"),
        products("category"),
        products("price"),
        partitionedOrders("quantity"),
        partitionedOrders("order_date"),
        partitionedOrders("status")
      )

    // ------------------------------------------------------------
    // 6. Join with customers
    // ------------------------------------------------------------

    val finalOrders = enrichedOrders
      .join(
        customers,
        enrichedOrders("customer_id") === customers("customer_id"),
        "inner"
      )
      .select(
        enrichedOrders("order_id"),
        enrichedOrders("customer_id"),
        customers("name").alias("customer_name"),
        customers("city"),
        customers("segment"),
        enrichedOrders("product_id"),
        enrichedOrders("product_name"),
        enrichedOrders("category"),
        enrichedOrders("price"),
        enrichedOrders("quantity"),
        enrichedOrders("order_date"),
        enrichedOrders("status")
      )
      .withColumn(
        "revenue",
        col("price") * col("quantity")
      )
      .cache()

    // ------------------------------------------------------------
    // 7. UDF - customer value classification
    // ------------------------------------------------------------

    val customerValueUDF = udf { revenue: Double =>
      if (revenue >= 100000) "HIGH_VALUE"
      else if (revenue >= 50000) "MEDIUM_VALUE"
      else "STANDARD_VALUE"
    }

    val classifiedOrders = finalOrders
      .withColumn(
        "order_value_class",
        customerValueUDF(col("revenue"))
      )

    // ------------------------------------------------------------
    // 8. Spark SQL
    // ------------------------------------------------------------

    classifiedOrders.createOrReplaceTempView("ecommerce_orders")

    println("\n========== SPARK SQL ==========")

    val sqlResult = spark.sql("""
      SELECT
        category,
        COUNT(*) AS order_count,
        SUM(quantity) AS units_sold,
        ROUND(SUM(revenue), 2) AS total_revenue
      FROM ecommerce_orders
      GROUP BY category
      ORDER BY total_revenue DESC
    """)

    sqlResult.show(false)

    // ------------------------------------------------------------
    // 9. Customer aggregation
    // ------------------------------------------------------------

    println("\n========== CUSTOMER REVENUE ==========")

    val customerRevenue = classifiedOrders
      .groupBy(
        "customer_id",
        "customer_name",
        "segment"
      )
      .agg(
        count("order_id").alias("order_count"),
        sum("revenue").alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    customerRevenue.show(false)

    // ------------------------------------------------------------
    // 10. Window function - customer ranking
    // ------------------------------------------------------------

    println("\n========== CUSTOMER RANKING ==========")

    val rankingWindow =
      Window.orderBy(desc("total_revenue"))

    val rankedCustomers = customerRevenue
      .withColumn(
        "revenue_rank",
        dense_rank().over(rankingWindow)
      )

    rankedCustomers.show(false)

    // ------------------------------------------------------------
    // 11. Running revenue by customer
    // ------------------------------------------------------------

    println("\n========== RUNNING REVENUE ==========")

    val customerWindow =
      Window
        .partitionBy("customer_id")
        .orderBy("order_date")

    val runningRevenue = classifiedOrders
      .withColumn(
        "running_revenue",
        sum("revenue").over(customerWindow)
      )
      .select(
        "customer_id",
        "customer_name",
        "order_id",
        "order_date",
        "revenue",
        "running_revenue"
      )

    runningRevenue.show(false)

    // ------------------------------------------------------------
    // 12. Product aggregation
    // ------------------------------------------------------------

    println("\n========== PRODUCT PERFORMANCE ==========")

    val productPerformance = classifiedOrders
      .groupBy(
        "product_id",
        "product_name",
        "category"
      )
      .agg(
        sum("quantity").alias("units_sold"),
        round(sum("revenue"), 2).alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    productPerformance.show(false)

    // ------------------------------------------------------------
    // 13. Partition output
    // ------------------------------------------------------------

    classifiedOrders
      .repartition(col("category"))
      .write
      .mode("overwrite")
      .partitionBy("category")
      .parquet("output/batch_orders")

    customerRevenue
      .write
      .mode("overwrite")
      .parquet("output/customer_revenue")

    // ------------------------------------------------------------
    // 14. Final metrics
    // ------------------------------------------------------------

    val totalRevenue =
      classifiedOrders
        .agg(sum("revenue"))
        .first()
        .getLong(0)
        .toDouble

    val totalUnits =
      classifiedOrders
        .agg(sum("quantity"))
        .first()
        .getLong(0)

    println("\n========== FINAL CAPSTONE METRICS ==========")
    println(s"Total source orders : ${orders.count()}")
    println(s"Cancelled orders    : ${cancelledOrders.value}")
    println(s"Completed orders    : ${completedOrders.count()}")
    println(s"Total units sold    : $totalUnits")
    println(f"Total revenue       : $$${totalRevenue}%.2f")
    println(s"Output directory    : output/batch_orders")
    println(s"Customer output     : output/customer_revenue")

    // ------------------------------------------------------------
    // 15. Cleanup
    // ------------------------------------------------------------

    completedOrders.unpersist()
    finalOrders.unpersist()

    spark.stop()
  }
}
