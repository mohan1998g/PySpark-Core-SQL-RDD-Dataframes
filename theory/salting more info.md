In PySpark, **we generally apply salting only to the skewed key(s), not to all the data.** However, the exact implementation depends on whether you're performing a join or an aggregation.

### 1. Example: Data skew in a join

Suppose you have two DataFrames:

**Large DataFrame**

| customer_id | amount |
|---|---:|
| 101 | 100 |
| 101 | 200 |
| 101 | 300 |
| 101 | 400 |
| 102 | 500 |
| 103 | 600 |

Suppose `customer_id = 101` has millions of records, while other customers have relatively few records.

When you join using `customer_id`, Spark may send a disproportionately large amount of data to one partition, causing a **data skew problem**.

### 2. Should we salt one key or all keys?

**Recommended approach: Salt only the skewed key(s).**

For example, suppose you identify `customer_id = 101` as the skewed key.

You can distribute its records across four salt values: `0, 1, 2, 3`.

| customer_id | amount | salt |
|---|---:|---:|
| 101 | 100 | 0 |
| 101 | 200 | 1 |
| 101 | 300 | 2 |
| 101 | 400 | 3 |
| 102 | 500 | NULL |
| 103 | 600 | NULL |

Now, records belonging to customer 101 can be distributed across multiple partitions.

**Important:** Adding a salt column alone doesn't guarantee that Spark distributes the data across partitions. You must incorporate the salt into the join or partitioning strategy.

### 3. How does salting work in a join?

Suppose you have:
- `df_large`: A large DataFrame with millions of records for customer 101.
- `df_small`: A smaller DataFrame containing customer information.

**Step 1: Add salt to the skewed key in the large DataFrame.**

```python
from pyspark.sql import functions as F

salt_buckets = 4

df_large_salted = df_large.withColumn(
    "salt",
    F.when(
        F.col("customer_id") == 101,
        F.floor(F.rand() * salt_buckets)
    ).otherwise(F.lit(0))
)
```

**Step 2: Replicate the matching row in the smaller DataFrame.**

```python
df_small_salted = df_small.withColumn(
    "salt",
    F.explode(
        F.when(
            F.col("customer_id") == 101,
            F.array(F.lit(0), F.lit(1), F.lit(2), F.lit(3))
        ).otherwise(F.array(F.lit(0)))
    )
)
```

**Step 3: Join using both columns.**

```python
result = df_large_salted.join(
    df_small_salted,
    ["customer_id", "salt"],
    "inner"
)
```

This allows records for customer 101 to join across four salt buckets instead of concentrating all matching records on one key.

*Note:* The example assumes customer 101 is the skewed key. In production, identify skewed keys from actual data rather than hardcoding them.

### 4. What about the other keys?

| Key | Data distribution | Apply salting? |
|---|---|---|
| 101 | Millions of records | Yes |
| 102 | 1,000 records | Usually no |
| 103 | 500 records | Usually no |
| 104 | 200 records | Usually no |

For non-skewed keys, we use a single salt value, `0`, on both sides of the join.

This avoids unnecessarily replicating data and increasing shuffle overhead.

### 5. Important distinction: Join vs. aggregation

| Scenario | Salting strategy |
|---|---|
| Join with a few skewed keys | Salt the skewed keys and replicate their matching rows on the other side |
| Aggregation with skewed keys | Distribute the skewed key across salt buckets, aggregate partially, then aggregate the partial results |
| Many keys are skewed | Salt all identified skewed keys, or consider another strategy |
| No significant skew | Salting is generally unnecessary |

**Interview answer:** "We typically apply salting only to skewed keys rather than all records. For joins, we distribute records for skewed keys across multiple salt buckets and replicate the corresponding rows on the smaller DataFrame. Non-skewed keys use a default salt value. This reduces data skew while minimizing unnecessary data replication and shuffle overhead."

**Next step:** Learn how salting differs between joins and `GROUP BY` aggregations, especially why aggregation salting requires a second aggregation stage.

Explore the practical trade-offs

- Learn how to choose salt buckets
- See salting for GROUP BY
