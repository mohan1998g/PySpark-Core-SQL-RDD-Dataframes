# Sample data for t1
data = [("India",), ("Srilanka",), ("Australia",), ("Pakistan",)]
columns = ["Country"]

# Create DataFrames for t1 and t2 (same data)
t1 = spark.createDataFrame(data, columns)
t2 = spark.createDataFrame(data, columns)

# Perform a self-join with a condition to avoid duplicates
result1 = (t1.alias("t1").join(t2.alias("t2"),col("t1.Country") > col("t2.Country"))
            .select(col("t1.Country").alias("Country1"), col("t2.Country").alias("Country2")))

result2 = (t1.alias("t1").join(t2.alias("t2"),col("t1.Country") < col("t2.Country"))
          .select(col("t1.Country").alias("Country1"), col("t2.Country").alias("Country2")))

# Show the result
result1.show()

+--------+---------+
|Country1| Country2|
+--------+---------+
|   India|Australia|
|Srilanka|    India|
|Srilanka|Australia|
|Srilanka| Pakistan|
|Pakistan|    India|
|Pakistan|Australia|
+--------+---------+

result2.show()

+---------+--------+
| Country1|Country2|
+---------+--------+
|    India|Srilanka|
|    India|Pakistan|
|Australia|   India|
|Australia|Srilanka|
|Australia|Pakistan|
| Pakistan|Srilanka|
+---------+--------+

alias("t1") and alias("t2"):

This creates two aliases for the DataFrames t1 and t2. It's useful for joining the same DataFrame with itself or when you need to distinguish between columns that come from the same DataFrame (like in a self-join).
Here, both t1 and t2 contain the list of countries, and by using aliases, you can refer to them as t1 and t2 separately.
join(t2.alias("t2"), col("t1.Country") < col("t2.Country")):

This performs an inner join between t1 and t2, but with a condition: it only keeps the rows where the value of t1.Country is lexicographically smaller than t2.Country.
The col("t1.Country") < col("t2.Country") condition ensures that only pairs of countries where the country from t1 comes before the country from t2 are retained. 
For example, "India" will only be paired with "Srilanka", "Australia", and "Pakistan", but not with itself or any country that comes lexicographically earlier.
.select(col("t1.Country").alias("Country1"), col("t2.Country").alias("Country2")):

After the join, this select statement picks the Country column from both t1 and t2.
It renames the columns to Country1 and Country2 for clarity, so the resulting DataFrame has two columns: Country1 and Country2.

What This Code Does:
It generates all possible pairs of different countries, where Country1 comes lexicographically before Country2.
This prevents reverse duplicate pairs like ("India", "Srilanka") and ("Srilanka", "India") from appearing; only the lexicographically smaller combination will be retained.


----------------------------------------------
# Define the schema
schema = StructType([
    StructField("teams", StringType(), True)
])

# Define the data
data = [
    ("India",),
    ("Pakistan",),
    ("SriLanka",)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()

joindf = (df.alias("t1").join(df.alias("t2"), col("t1.teams") > col("t2.teams"))
          .select(col("t1.teams").alias("Country1"), col("t2.teams").alias("Country2"))
          .withColumn("team", expr("concat(Country1,' ' ,'Vs',' ', Country2)"))
          .select("team"))
joindf.show()
