# Day 30 - Final E-Commerce Capstone

An end-to-end **Apache Spark + Scala e-commerce data engineering capstone** combining batch analytics and real-time streaming.

This project brings together the major Spark concepts covered throughout the 30-day learning journey, including Spark SQL, DataFrames, joins, aggregations, window functions, UDFs, broadcast variables, accumulators, caching, partitioning, Parquet output, and DStreams.

---

## Project Overview

The project processes e-commerce customer, product, and order data in two modes:

### Batch Processing
The batch application:

- Reads customers, products, and orders from CSV files
- Performs data quality analysis
- Counts cancelled orders using an accumulator
- Filters completed orders
- Caches frequently used data
- Repartitions completed orders by customer
- Uses a broadcast join for product enrichment
- Joins orders with customer information
- Calculates order revenue
- Uses a UDF to classify order values
- Performs Spark SQL aggregations
- Calculates customer revenue
- Uses window functions for customer ranking
- Calculates running revenue per customer
- Produces product performance metrics
- Writes partitioned Parquet output

### Streaming Processing
The streaming application uses Spark DStreams to:

- Monitor `data/stream`
- Parse incoming order events
- Validate streaming records
- Count invalid records with an accumulator
- Filter completed orders
- Aggregate customer revenue
- Use a broadcast customer lookup
- Aggregate order status counts
- Process data in 5-second micro-batches

---

## Architecture

```text
                    E-COMMERCE DATA
                          |
              +-----------+-----------+
              |                       |
              v                       v
        Batch Processing       Streaming Processing
              |                       |
       CSV Raw Data              DStream Input
              |                       |
       Spark DataFrames          5-sec Batches
              |                       |
       +------+-------+          +----+-----+
       |              |          |          |
     Joins       Transformations Broadcast  Accumulator
       |              |          |          |
       +------+-------+----------+----------+
              |
              v
        Spark Analytics
              |
      +-------+--------+
      |                |
      v                v
 Customer Analytics  Product Analytics
      |                |
      +-------+--------+
              |
              v
        Parquet Output
Technology Stack
Technology	Version
Scala	2.12.18
Apache Spark	3.5.1
Spark Core	3.5.1
Spark SQL	3.5.1
Spark Streaming	3.5.1
SBT	2.x
Java	17
Platform	WSL2 / Linux
Project Structure
Day30-Final-E-Commerce-Capstone/
│
├── README.md
├── .gitignore
├── build.sbt
│
├── data/
│   ├── raw/
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   └── orders.csv
│   │
│   └── stream/
│
├── interview/
│   └── 20-interview-questions.md
│
├── project/
│   └── build.properties
│
└── src/
    └── main/
        └── scala/
            └── com/
                └── day30/
                    ├── BatchCapstoneApp.scala
                    └── StreamingCapstoneApp.scala

Generated directories such as target/, output/, .bsp/, checkpoints, logs, and IDE metadata are excluded using .gitignore.

Batch Pipeline
Input Data
Customers
customer_id,name,city,segment
C001,Aarav,Hyderabad,Premium
C002,Ananya,Bangalore,Regular
C003,Rahul,Chennai,Premium
C004,Sneha,Mumbai,Regular
C005,Vikram,Delhi,Enterprise
C006,Meera,Pune,Regular
Products
product_id,product_name,category,price
P001,Laptop,Electronics,75000
P002,Phone,Electronics,45000
P003,Headphones,Accessories,5000
P004,Keyboard,Accessories,3000
P005,Monitor,Electronics,18000
P006,Mouse,Accessories,1500
Orders

The order dataset contains 15 source orders across multiple customers, products, dates, and statuses.

Batch Processing Concepts
1. Data Quality

Cancelled orders are counted using a Spark accumulator.

val cancelledOrders =
  spark.sparkContext.longAccumulator("Cancelled Orders")
2. Filtering

Only completed orders are used for revenue analytics.

.filter(col("status") === "COMPLETED")
3. Caching

Frequently reused completed orders are cached.

.cache()
4. Partitioning

Completed orders are repartitioned by customer:

.repartition(4, col("customer_id"))

This demonstrates partition tuning and data distribution.

5. Broadcast Join

The product reference data is broadcast to reduce shuffle requirements for the product enrichment join.

6. Customer Join

Order data is joined with customer information to enrich transactions with:

Customer name
City
Segment
7. Revenue Calculation
revenue = price × quantity
8. UDF

A Spark UDF classifies order values into business categories.

9. Spark SQL

Category-level analytics are performed using Spark SQL aggregations.

10. Window Functions

Customer revenue is ranked using a global dense-rank window.

Running revenue is calculated using a customer-partitioned window.

Batch Results

The completed batch pipeline produced the following results:

========== SOURCE COUNTS ==========
Customers: 6
Products : 6
Orders   : 15

========== DATA QUALITY ==========
Cancelled orders: 1
Completed orders: 14

========== PARTITIONING ==========
Partitions after repartition: 4
Category Performance
Electronics | 8 orders | 13 units | $543000
Accessories | 6 orders | 19 units | $68000
Customer Revenue
C005 Vikram   Enterprise | 2 orders | $204000
C002 Ananya   Regular    | 3 orders | $175000
C001 Aarav    Premium    | 3 orders | $118000
C003 Rahul    Premium    | 3 orders | $96000
C006 Meera    Regular    | 2 orders | $15000
C004 Sneha    Regular    | 1 order  | $3000
Customer Ranking
C005 -> Rank 1
C002 -> Rank 2
C001 -> Rank 3
C003 -> Rank 4
C006 -> Rank 5
C004 -> Rank 6
Product Performance
P001 Laptop      | 4 units  | $300000
P002 Phone       | 3 units  | $135000
P005 Monitor     | 6 units  | $108000
P003 Headphones  | 10 units | $50000
P006 Mouse       | 6 units  | $9000
P004 Keyboard    | 3 units  | $9000
Final Batch Metrics
Total source orders : 15
Cancelled orders    : 1
Completed orders    : 14
Total units sold    : 32
Total revenue       : $611000.00
Streaming Pipeline

The streaming application uses:

StreamingContext(conf, Seconds(5))

and monitors:

data/stream

New files placed into the directory are processed as streaming micro-batches.

Streaming Input Format
order_id,customer_id,product_id,quantity,amount,status

Example:

S001,C001,P001,1,75000,COMPLETED
S002,C002,P002,2,90000,COMPLETED
S003,C005,P005,1,18000,COMPLETED
S004,C004,P004,1,3000,CANCELLED
Streaming Features
Broadcast Variable

Customer IDs are mapped to customer names using a broadcast variable.

C001 -> Aarav
C002 -> Ananya
C003 -> Rahul
C004 -> Sneha
C005 -> Vikram
C006 -> Meera
Accumulator

Malformed records are counted using:

Invalid Streaming Records
Cache

Completed streaming orders are cached before downstream analytics.

reduceByKey

Customer revenue is aggregated using reduceByKey.

Status Aggregation

The streaming application also calculates:

COMPLETED -> count
CANCELLED -> count
Streaming Test Results
Streaming Batch 1

Input contained:

4 completed orders
1 cancelled order
1 invalid record

Result:

========== STREAMING CUSTOMER REVENUE ==========

C002  Ananya     Revenue: $90000.00
C001  Aarav      Revenue: $75000.00
C005  Vikram     Revenue: $18000.00
C003  Rahul      Revenue: $10000.00

========== STREAMING ORDER STATUS ==========

CANCELLED -> 1
COMPLETED -> 4
Streaming Batch 2

Input contained:

4 completed orders
1 cancelled order

Result:

========== STREAMING CUSTOMER REVENUE ==========

C002  Ananya     Revenue: $75000.00
C005  Vikram     Revenue: $45000.00
C001  Aarav      Revenue: $36000.00
C006  Meera      Revenue: $7500.00

========== STREAMING ORDER STATUS ==========

CANCELLED -> 1
COMPLETED -> 4

The streaming application successfully processed both micro-batches.

How to Run
1. Compile
sbt compile
2. Run Batch Pipeline
JAVA_TOOL_OPTIONS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED" sbt "runMain com.day30.BatchCapstoneApp"

Batch output is written to:

output/batch_orders
output/customer_revenue
3. Run Streaming Pipeline

Start the application:

JAVA_TOOL_OPTIONS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED" sbt "runMain com.day30.StreamingCapstoneApp"

The application watches:

data/stream

In another terminal, create a new CSV file:

cat > data/stream/stream_batch.csv <<'EOF'
S001,C001,P001,1,75000,COMPLETED
S002,C002,P002,2,90000,COMPLETED
S003,C005,P005,1,18000,COMPLETED
S004,C004,P004,1,3000,CANCELLED
