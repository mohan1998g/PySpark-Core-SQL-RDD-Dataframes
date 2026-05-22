# Step 2: Define the schema
schema = StructType([
        StructField("Range", IntegerType(), True),
        StructField("Number", IntegerType(), True)
])

# Step 3: Create the data
data = [
        (90, 2),
        (60, 3),
        (70, 5),
        (80, 1)
]

# Step 4: Create DataFrame
df = spark.createDataFrame(data, schema)

# Step 5: Show the DataFrame
df.show()

df2 = df.withColumn("total", sum("Number").over(Window.orderBy(col("Range").desc())))
df2.show()

+-----+------+
|Range|Number|
+-----+------+
|   90|     2|
|   60|     3|
|   70|     5|
|   80|     1|
+-----+------+


+-----+------+-----+
|Range|Number|total|
+-----+------+-----+
|   90|     2|    2|
|   80|     1|    3|
|   70|     5|    8|
|   60|     3|   11|
+-----+------+-----+

-------------------------------------------------------------------------------------

# Step 2: Define the schema
schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("sal", IntegerType(), True)
])

# Step 3: Create the data
data = [
        (1, 'A', 1000),
        (2, 'B', 2000),
        (3, 'C', 3000),
        (4, 'D', 4000)
]

# Step 4: Create DataFrame
df = spark.createDataFrame(data, schema)

# Step 5: Show the DataFrame
df.show()

df2 = df.withColumn("total", sum("sal").over(Window.orderBy(col("id"))))
df2.show()

+---+----+----+
| id|name| sal|
+---+----+----+
|  1|   A|1000|
|  2|   B|2000|
|  3|   C|3000|
|  4|   D|4000|
+---+----+----+



+---+----+----+-----+
| id|name| sal|total|
+---+----+----+-----+
|  1|   A|1000| 1000|
|  2|   B|2000| 3000|
|  3|   C|3000| 6000|
|  4|   D|4000|10000|
+---+----+----+-----+

-----------------------------------------------------------------------------

# Cumulative sum over a partition
# Define the schema
schema = StructType([
    StructField("pid", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("price", IntegerType(), True)
])

# Define the data
data = [
    (1, "26-May", 100),
    (1, "27-May", 200),
    (1, "28-May", 300),
    (2, "29-May", 400),
    (3, "30-May", 500),
    (3, "31-May", 600)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()

wn = Window.partitionBy("pid").orderBy("price")
finaldf = df.withColumn("new_price", sum("price").over(wn))
finaldf.show()

# Through SQL
df.createOrReplaceTempView("ordertab")
spark.sql("select pid,date,price, sum(price) over(partition by(pid) order by(price)) as new_price from ordertab").show()
