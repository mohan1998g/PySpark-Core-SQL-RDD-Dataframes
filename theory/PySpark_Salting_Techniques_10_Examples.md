# PySpark Data Skew & Salting --- 10 Practical Examples

## Purpose

This handbook explains **10 practical salting techniques in PySpark**
with sample data, code, expected behavior, and interview notes.

> **Important:** Salting is not the first thing you should apply to
> every skew problem. In modern Spark, try **AQE skew-join
> optimization** first. Use salting when a hot key remains problematic
> or when you need explicit control over how the workload is
> distributed.

------------------------------------------------------------------------

# 1. Basic Salting for a Skewed `groupBy`

## Scenario

Suppose one customer has millions of transactions:

``` text
customer_id    amount
-----------    ------
101            100
101            200
101            300
101            400
102            500
103            700
```

A normal aggregation:

``` python
df.groupBy("customer_id").sum("amount")
```

can send all records for customer `101` toward the same aggregation
partition.

## Create sample data

``` python
data = [
    (101, 100),
    (101, 200),
    (101, 300),
    (101, 400),
    (102, 500),
    (103, 700)
]

df = spark.createDataFrame(
    data,
    ["customer_id", "amount"]
)
```

## Add a random salt

``` python
from pyspark.sql.functions import floor, rand

salt_count = 4

df_salted = df.withColumn(
    "salt",
    floor(rand() * salt_count).cast("int")
)

df_salted.show()
```

Possible output:

``` text
+-----------+------+----+
|customer_id|amount|salt|
+-----------+------+----+
|101        |100   |0   |
|101        |200   |3   |
|101        |300   |1   |
|101        |400   |2   |
|102        |500   |1   |
|103        |700   |0   |
+-----------+------+----+
```

## Aggregate in two stages

``` python
partial = (
    df_salted
    .groupBy("customer_id", "salt")
    .sum("amount")
)

result = (
    partial
    .groupBy("customer_id")
    .sum("sum(amount)")
)

result.show()
```

## Why it works

Instead of one hot key:

``` text
101
 |
 +---------------- millions of rows
```

we create:

``` text
101_0
101_1
101_2
101_3
```

The first aggregation can therefore be distributed across multiple
partitions.

### Interview point

**Salting for aggregation usually requires a two-stage aggregation:**

``` text
Original data
     ↓
Add salt
     ↓
groupBy(key, salt)
     ↓
Partial aggregation
     ↓
groupBy(key)
     ↓
Final aggregation
```

------------------------------------------------------------------------

# 2. Salting a Skewed Join

This is the most commonly discussed salting interview scenario.

## Large table

``` python
orders = spark.createDataFrame([
    (101, 1000),
    (101, 2000),
    (101, 3000),
    (102, 500),
    (103, 700)
], ["customer_id", "amount"])
```

Imagine the real data contains:

``` text
customer_id = 101 → 500 million rows
customer_id = 102 → 100 rows
customer_id = 103 → 50 rows
```

## Small dimension table

``` python
customers = spark.createDataFrame([
    (101, "Mohan"),
    (102, "Rahul"),
    (103, "John")
], ["customer_id", "name"])
```

A normal join:

``` python
result = orders.join(
    customers,
    "customer_id"
)
```

can suffer from skew when `101` is extremely frequent.

## Step 1 --- Add salt to the large table

``` python
from pyspark.sql.functions import floor, rand

N = 10

orders_salted = orders.withColumn(
    "salt",
    floor(rand() * N).cast("int")
)
```

Now the hot key is distributed as:

``` text
101 + 0
101 + 1
101 + 2
...
101 + 9
```

## Step 2 --- Replicate the small table

The small table must contain every possible salt value.

``` python
from pyspark.sql.functions import explode, sequence

customers_salted = (
    customers
    .withColumn("salt", explode(sequence(
        lit(0),
        lit(N - 1)
    )))
)
```

The result conceptually becomes:

``` text
customer_id    name     salt
-----------    -------  ----
101            Mohan    0
101            Mohan    1
101            Mohan    2
...
101            Mohan    9
102            Rahul    0
102            Rahul    1
...
```

## Step 3 --- Join using both columns

``` python
result = orders_salted.join(
    customers_salted,
    ["customer_id", "salt"],
    "inner"
)
```

## Why replicate the small table?

Suppose the order:

``` text
101 → salt 7
```

needs to find customer `101`.

The dimension table must therefore contain:

``` text
101, Mohan, 7
```

Otherwise that row would not match.

### Interview answer

> In a salted join, we add a salt value to the skewed large-side key and
> replicate the smaller-side matching key across all salt values. We
> then join on `(original_key, salt)`.

------------------------------------------------------------------------

# 3. Deterministic Salting Using `hash()`

Random salt is not always appropriate.

Suppose we want the same record to consistently receive the same salt.

``` python
from pyspark.sql.functions import hash, pmod, lit

N = 10

df_salted = df.withColumn(
    "salt",
    pmod(hash("transaction_id"), lit(N))
)
```

For example:

``` text
transaction_id    salt
--------------    ----
10001             3
10002             8
10003             1
10004             6
```

## Why use deterministic salt?

The same transaction always gets the same salt.

This can be useful when:

-   you need reproducibility
-   you need to derive the salt on multiple DataFrames
-   the same logical record must consistently map to the same salted
    bucket

### Important

Deterministic salting does **not automatically fix a single hot key** if
the salt is derived only from that same hot key.

Bad:

``` python
pmod(hash("customer_id"), 10)
```

If `customer_id = 101` is the hot key, every `101` record gets the same
salt.

Better:

``` python
pmod(hash("transaction_id"), 10)
```

when transaction IDs are sufficiently distributed.

------------------------------------------------------------------------

# 4. Salting with `monotonically_increasing_id()`

Another technique is to create a pseudo-unique row identifier and derive
a salt from it.

``` python
from pyspark.sql.functions import (
    monotonically_increasing_id,
    pmod,
    lit
)

N = 10

df_salted = (
    df
    .withColumn(
        "_row_id",
        monotonically_increasing_id()
    )
    .withColumn(
        "salt",
        pmod("_row_id", lit(N))
    )
)
```

Now records can be spread across:

``` text
salt 0
salt 1
salt 2
...
salt 9
```

## When is this useful?

It is useful when:

-   you need a deterministic-ish salt within the generated dataset
-   no suitable high-cardinality column is available
-   you need to distribute rows of a hot key

## Important caution

`monotonically_increasing_id()` does **not** generate globally
consecutive integers. It is unique and monotonically increasing within
Spark's distributed execution semantics, but values can have large gaps.

------------------------------------------------------------------------

# 5. Salting with `xxhash64()`

For large-scale PySpark workloads, `xxhash64()` is often a convenient
way to derive a deterministic hash.

``` python
from pyspark.sql.functions import xxhash64, pmod, lit

N = 16

df_salted = df.withColumn(
    "salt",
    pmod(
        xxhash64("transaction_id"),
        lit(N)
    )
)
```

Conceptually:

``` text
transaction_id → hash → modulo N → salt
```

Example:

``` text
10001 → hash → 3
10002 → hash → 12
10003 → hash → 7
10004 → hash → 1
```

## Why use a hash?

Hashing can distribute high-cardinality identifiers more evenly.

This is particularly useful when you have:

``` text
hot_key + high_cardinality_secondary_key
```

and want to derive a repeatable salt.

### Interview point

A common pattern is:

``` python
pmod(xxhash64("high_cardinality_column"), lit(N))
```

------------------------------------------------------------------------

# 6. Salting Only the Hot Keys

You do **not always need to salt every key**.

Suppose:

``` text
customer_id = 101 → 500 million rows
customer_id = 102 → 100 rows
customer_id = 103 → 50 rows
```

Only `101` is problematic.

We can salt only the hot key.

``` python
from pyspark.sql.functions import when, floor, rand, lit

N = 10

df_salted = df.withColumn(
    "salt",
    when(
        col("customer_id") == 101,
        floor(rand() * N)
    ).otherwise(lit(0))
)
```

Now:

``` text
101 → salts 0 through 9
102 → salt 0
103 → salt 0
```

## Why is this better?

If you salt every key:

``` text
102 → 10 copies
103 → 10 copies
104 → 10 copies
...
```

you can unnecessarily increase data volume.

Hot-key-only salting minimizes that overhead.

### Interview answer

> If only a few keys are skewed, salt only those keys instead of
> multiplying every key.

------------------------------------------------------------------------

# 7. Different Salt Counts for Different Hot Keys

Sometimes one key has:

``` text
101 → 1 billion rows
```

while another has:

``` text
202 → 100 million rows
```

and normal keys have very little data.

Using the same salt count for everything may be inefficient.

We can assign salt ranges based on the key.

``` python
from pyspark.sql.functions import when, floor, rand, lit

df_salted = df.withColumn(
    "salt",
    when(
        col("customer_id") == 101,
        floor(rand() * 20)
    )
    .when(
        col("customer_id") == 202,
        floor(rand() * 10)
    )
    .otherwise(
        lit(0)
    )
)
```

Conceptually:

``` text
101 → 20 buckets
202 → 10 buckets
normal keys → 1 bucket
```

## Why?

The more data a hot key contains, the more buckets it may require.

### Interview point

The salt count is a **tuning parameter**.

Too small:

``` text
1 billion rows / 2 buckets
```

may still leave huge partitions.

Too large:

``` text
1 billion rows / 10,000 buckets
```

can create too many tiny tasks.

------------------------------------------------------------------------

# 8. Salting a Join with a Known Hot Key List

Instead of detecting hot keys manually in code, you may have a list of
known hot keys.

``` python
hot_keys = [101, 202, 303]
```

Then:

``` python
from pyspark.sql.functions import when, floor, rand, lit

N = 10

df_salted = df.withColumn(
    "salt",
    when(
        col("customer_id").isin(hot_keys),
        floor(rand() * N)
    ).otherwise(
        lit(0)
    )
)
```

Now:

``` text
101 → 0-9
202 → 0-9
303 → 0-9
normal keys → 0
```

## More realistic approach

Find hot keys first:

``` python
hot_keys_df = (
    df.groupBy("customer_id")
      .count()
      .filter(col("count") > 1000000)
)
```

Then use the hot-key DataFrame to drive a more scalable solution rather
than collecting millions of keys to the driver.

For a small list of known hot keys, however:

``` python
hot_keys = [101, 202, 303]
```

is perfectly reasonable.

------------------------------------------------------------------------

# 9. Salting a Composite Join Key

Skew does not always happen on a single column.

Suppose the join is:

``` python
df1.join(
    df2,
    ["country", "customer_id"]
)
```

and one combination is extremely frequent:

``` text
country = IN
customer_id = 101
```

We can salt the composite key.

## Large side

``` python
N = 8

df1_salted = df1.withColumn(
    "salt",
    floor(rand() * N)
)
```

## Small side

``` python
df2_salted = (
    df2
    .withColumn(
        "salt",
        explode(sequence(
            lit(0),
            lit(N - 1)
        ))
    )
)
```

## Join

``` python
result = df1_salted.join(
    df2_salted,
    ["country", "customer_id", "salt"],
    "inner"
)
```

## Why?

The effective join key becomes:

``` text
(country, customer_id, salt)
```

instead of:

``` text
(country, customer_id)
```

This gives Spark additional distribution keys.

### Interview scenario

> "The join is on three columns and the skew occurs for one specific
> combination. Can you salt it?"

Yes. Salt can be added to a composite join key exactly the same way.

------------------------------------------------------------------------

# 10. Salting a Join + Broadcast Strategy

Salting is not always the best solution.

If one side of the join is genuinely small, a **broadcast join** may be
much better.

Suppose:

``` text
fact table       → 500 GB
dimension table  → 50 MB
```

Instead of salting:

``` python
from pyspark.sql.functions import broadcast

result = fact.join(
    broadcast(dimension),
    "customer_id"
)
```

Spark can broadcast the small table to executors and avoid a large
shuffle join.

## Why include this as a salting example?

Because in real projects, the correct response to skew is not always:

> "Use salting."

You should first ask:

1.  Can I broadcast the small side?
2.  Is AQE enabled?
3.  Is the skew caused by a few hot keys?
4.  Is salting actually necessary?

### Interview answer

> If the smaller side fits safely in executor memory, a broadcast join
> may eliminate the shuffle and be preferable to salting. Salting is
> more useful when broadcasting is not feasible.

------------------------------------------------------------------------

# 11. Advanced Example --- Salting a Skewed Aggregation with a Deterministic Salt

Here is a more realistic aggregation example.

## Data

``` python
data = [
    ("US", "A", 100),
    ("US", "A", 200),
    ("US", "A", 300),
    ("IN", "B", 400),
    ("IN", "C", 500)
]

df = spark.createDataFrame(
    data,
    ["country", "product", "amount"]
)
```

Suppose:

``` text
country = US
product = A
```

has hundreds of millions of records.

Create a deterministic salt based on a high-cardinality column:

``` python
from pyspark.sql.functions import (
    xxhash64,
    pmod,
    lit
)

N = 8

df_salted = df.withColumn(
    "salt",
    pmod(
        xxhash64("product"),
        lit(N)
    )
)
```

### Important limitation

If `product = A` is the hot key and we hash **only `product`**, every
`A` record gets the same salt.

Therefore, use a column with enough variation, such as:

``` python
df_salted = df.withColumn(
    "salt",
    pmod(
        xxhash64("transaction_id"),
        lit(N)
    )
)
```

Then:

``` python
partial = (
    df_salted
    .groupBy(
        "country",
        "product",
        "salt"
    )
    .sum("amount")
)

result = (
    partial
    .groupBy(
        "country",
        "product"
    )
    .sum("sum(amount)")
)
```

This is a classic **two-phase salted aggregation**.

------------------------------------------------------------------------

# 12. Advanced Example --- Random Salt vs Deterministic Salt

This distinction is important in interviews.

## Random salt

``` python
from pyspark.sql.functions import floor, rand

df = df.withColumn(
    "salt",
    floor(rand() * 10)
)
```

Advantages:

-   simple
-   distributes repeated records of a hot key
-   useful for salted aggregation/join techniques

Disadvantages:

-   not deterministic
-   the same row can receive a different salt on another execution

------------------------------------------------------------------------

## Deterministic salt

``` python
from pyspark.sql.functions import xxhash64, pmod, lit

df = df.withColumn(
    "salt",
    pmod(
        xxhash64("transaction_id"),
        lit(10)
    )
)
```

Advantages:

-   reproducible mapping for the same input key
-   useful when the same salt needs to be derived independently on
    different DataFrames

Disadvantages:

-   requires a sufficiently distributed column
-   hashing the skewed key itself does not fix the skew

------------------------------------------------------------------------

# 13. Complete Interview Pattern --- Salted Join

Here is the pattern you should remember.

## Large table

``` python
from pyspark.sql.functions import (
    col,
    floor,
    rand,
    lit,
    explode,
    sequence
)

N = 10

large_salted = large_df.withColumn(
    "salt",
    when(
        col("customer_id").isin(hot_keys),
        floor(rand() * N)
    ).otherwise(
        lit(0)
    )
)
```

## Small table

For a small dimension table:

``` python
small_salted = (
    small_df
    .withColumn(
        "salt",
        explode(
            sequence(
                lit(0),
                lit(N - 1)
            )
        )
    )
)
```

## Join

``` python
result = large_salted.join(
    small_salted,
    ["customer_id", "salt"],
    "inner"
)
```

## Important correction for non-hot keys

If you salt only hot keys, the small side should also be constructed
carefully so that:

``` text
hot key      → all salt values
normal key   → salt 0 only
```

Otherwise you may unnecessarily multiply the small table.

------------------------------------------------------------------------

# 14. Choosing the Number of Salt Buckets

There is no universal value such as:

``` python
N = 10
```

The correct number depends on:

-   amount of skew
-   number of executor cores
-   number of shuffle partitions
-   size of the hot key
-   available executor memory
-   target task size

A rough conceptual approach:

``` text
Hot key size
     ↓
Estimate desired partition size
     ↓
Determine number of salt buckets
     ↓
Test in Spark UI
     ↓
Adjust
```

For example:

``` text
Hot key = 100 GB
Desired chunk = 1 GB

100 / 1 = approximately 100 salt buckets
```

This is only a starting estimate. Always validate against the actual
workload.

------------------------------------------------------------------------

# 15. Salting vs AQE

This is a very common interview question.

  -----------------------------------------------------------------------
  Technique               AQE                     Salting
  ----------------------- ----------------------- -----------------------
  Automatic               Yes                     No

  Changes key/data        No                      Yes

  Handles skewed joins    Yes                     Yes

  Runtime detection       Yes                     No

  Manual control          Limited                 High

  Requires data           No                      Sometimes
  replication                                     

  Good first option       Yes                     Usually after AQE

  Useful for extreme hot  Sometimes               Yes
  keys                                            

  Useful for skewed       Limited compared with   Yes
  aggregation             salted aggregation      
  -----------------------------------------------------------------------

A good production decision tree is:

``` text
              Data skew
                  |
                  v
           Enable AQE
                  |
                  v
          Inspect Spark UI
                  |
       +----------+----------+
       |                     |
   AQE solves            Still skewed
       |                     |
       v                     v
      Done          Check broadcast possibility
                             |
                     +-------+-------+
                     |               |
                  Possible        Not possible
                     |               |
                     v               v
                 Broadcast       Salt hot keys
```

------------------------------------------------------------------------

# 16. Common Mistakes with Salting

## Mistake 1 --- Hashing the skewed key itself

Bad:

``` python
pmod(xxhash64("customer_id"), lit(10))
```

If:

``` text
customer_id = 101
```

is the hot key, every `101` record receives the same salt.

Use a high-cardinality secondary column instead:

``` python
pmod(
    xxhash64("transaction_id"),
    lit(10)
)
```

------------------------------------------------------------------------

## Mistake 2 --- Salting every key unnecessarily

Bad:

``` text
Every customer × 10 salt values
```

This increases data volume.

Better:

``` text
Hot customers → salted
Normal customers → salt 0
```

------------------------------------------------------------------------

## Mistake 3 --- Forgetting to replicate the small side

For a salted join:

``` python
large.join(
    small,
    ["customer_id", "salt"]
)
```

the small side must have matching salt values.

------------------------------------------------------------------------

## Mistake 4 --- Choosing an excessively large salt count

If:

``` text
N = 10,000
```

you may create many tiny tasks and increase overhead.

------------------------------------------------------------------------

## Mistake 5 --- Using salting when broadcast is better

If:

``` text
large table = 500 GB
small table = 20 MB
```

consider:

``` python
broadcast(small_df)
```

before implementing a complicated salted join.

------------------------------------------------------------------------

# 17. The Most Important 10 Patterns to Remember

For interviews, remember these ten:

### Pattern 1 --- Basic salted aggregation

``` python
df.withColumn(
    "salt",
    floor(rand() * N)
)
```

then:

``` python
groupBy("key", "salt")
```

and finally:

``` python
groupBy("key")
```

### Pattern 2 --- Salted join

``` python
large.join(
    small,
    ["key", "salt"]
)
```

### Pattern 3 --- Replicate small side

``` python
explode(sequence(lit(0), lit(N - 1)))
```

### Pattern 4 --- Deterministic salt

``` python
pmod(xxhash64("id"), lit(N))
```

### Pattern 5 --- Salt only hot keys

``` python
when(
    col("key").isin(hot_keys),
    floor(rand() * N)
).otherwise(lit(0))
```

### Pattern 6 --- Different salt counts

``` python
when(col("key") == 101, floor(rand() * 20))
.when(col("key") == 202, floor(rand() * 10))
```

### Pattern 7 --- Composite-key salting

``` python
join(
    ["country", "customer_id", "salt"]
)
```

### Pattern 8 --- Hash-based salt

``` python
pmod(xxhash64("transaction_id"), lit(N))
```

### Pattern 9 --- AQE before salting

``` python
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true"
)
```

and:

``` python
spark.conf.set(
    "spark.sql.adaptive.skewJoin.enabled",
    "true"
)
```

### Pattern 10 --- Broadcast instead of salting

``` python
large.join(
    broadcast(small),
    "key"
)
```

when the small side is genuinely small enough.

------------------------------------------------------------------------

# 18. Interview Questions Based on These Examples

## Q1. What is salting?

**Answer:** Salting is a technique where an additional value is added to
a skewed key so that records belonging to a hot key can be distributed
across multiple partitions.

## Q2. Why is salting required?

**Answer:** To prevent a small number of hot keys from creating huge
shuffle partitions and straggler tasks.

## Q3. Does salting eliminate the original key?

**Answer:** No. It adds another component to the key. The original key
is retained so the final result can be reconstructed.

## Q4. How do you salt a join?

**Answer:** Add a salt to the skewed large side and replicate the
smaller side across all required salt values, then join on
`(original_key, salt)`.

## Q5. Why do we replicate the small table?

**Answer:** Because a large-side record with salt `7` needs a matching
small-side row with the same original key and salt `7`.

## Q6. Does AQE make salting unnecessary?

**Answer:** No. AQE can automatically mitigate skewed shuffle
partitions, especially skewed joins, but severe skew or other skew
patterns may still require manual techniques such as salting.

## Q7. What is the biggest disadvantage of salting?

**Answer:** It increases complexity and can increase data volume,
especially because the small side may need to be replicated.

## Q8. How do you decide the salt count?

**Answer:** Based on hot-key size, target partition size, executor
resources, and Spark UI observations. There is no universal number.

## Q9. Should we salt all keys?

**Answer:** Usually no. If only a few keys are hot, salt only those
keys.

## Q10. What is the alternative to salting when one table is small?

**Answer:** A broadcast join may be preferable because it can avoid the
large shuffle entirely.

------------------------------------------------------------------------

# Final Mental Model

Remember this:

``` text
                    DATA SKEW
                       |
                       v
                 Identify hot key
                       |
                       v
              Is AQE enabled?
                 /           \
               YES            NO
                |              |
                v              v
          Check Spark UI    Enable AQE
                |
                v
        Is skew still severe?
             /        \
           NO          YES
           |            |
          DONE          v
                 Is small side
                 broadcastable?
                    /      \
                  YES       NO
                   |         |
                   v         v
              Broadcast    SALTING
                             |
                    +--------+--------+
                    |                 |
              Salt large side   Replicate small
                    |                 |
                    +--------+--------+
                             |
                             v
                    Join on key + salt
```

**The core idea:** AQE is automatic runtime optimization; **salting is
deliberate data redistribution**. They are complementary techniques, not
mutually exclusive.
