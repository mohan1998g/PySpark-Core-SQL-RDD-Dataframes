# AQE vs Catalyst Optimizer in Apache Spark --- Extremely Detailed Guide

> **Scope:** PySpark / Spark SQL, with emphasis on interview preparation
> and real-world performance tuning.
>
> **Spark version note:** This guide reflects the current Spark
> documentation available for Spark 4.2.x. AQE has been enabled by
> default since Spark 3.2.0. Exact rules, defaults, and physical-plan
> details can vary by Spark version. Always verify version-specific
> behavior in your cluster.

------------------------------------------------------------------------

# 1. Executive Summary

The easiest way to understand the relationship is:

``` text
                    PySpark / Spark SQL
                            |
                            v
                 Parsed Logical Plan
                            |
                            v
                       Analyzer
                            |
                            v
                 Resolved Logical Plan
                            |
                            v
                 Catalyst Optimizer
                            |
                            v
                 Optimized Logical Plan
                            |
                            v
                  Physical Planning
                            |
                            v
                  Initial Physical Plan
                            |
                            v
             +----------------------------+
             | Adaptive Query Execution   |
             |            AQE             |
             +----------------------------+
                            |
                execute a query stage
                            |
                            v
                  Runtime Statistics
                            |
                            v
                    Re-optimize
                            |
                            v
                 Better Physical Plan
                            |
                            v
                         Execute
```

The central distinction is:

> **Catalyst mostly has to make decisions before it knows the exact
> runtime shape of the data. AQE can make or change execution decisions
> after Spark has observed actual runtime statistics.**

That difference explains almost everything about AQE.

------------------------------------------------------------------------

# 2. Catalyst vs AQE in One Sentence

### Catalyst

> "Given the query, schema, metadata, statistics available before
> execution, and optimizer rules, what is a good plan?"

### AQE

> "Now that I have actually executed part of the query and know the real
> sizes and distributions, can I change the remaining plan to something
> better?"

------------------------------------------------------------------------

# 3. Why AQE Was Needed

Consider:

``` python
result = (
    employees
    .join(departments, "dept_id")
    .groupBy("department")
    .count()
)
```

Suppose Spark initially estimates:

``` text
employees     = 500 GB
departments   = 800 MB
```

A reasonable initial plan may be:

``` text
SortMergeJoin
```

But suppose a filter earlier in the query actually reduces `departments`
to:

``` text
2 MB
```

The original planner could not know that exact runtime result before
executing the filter.

AQE can observe:

``` text
Actual departments after filtering = 2 MB
```

and potentially change:

``` text
SortMergeJoin
```

to:

``` text
BroadcastHashJoin
```

This is the fundamental gap AQE addresses.

------------------------------------------------------------------------

# 4. The Biggest Gap Between Static Optimization and Runtime Optimization

Before execution, Spark may know:

``` text
Schema
File metadata
Catalog statistics
Column statistics, if available
Partition information
Table statistics
Hints
```

But it may NOT know accurately:

``` text
Actual number of rows after a filter
Actual size after compression/decompression effects
Actual size of a shuffle partition
Actual distribution of rows across partitions
Actual skew
Actual post-filter table size
Actual runtime partition sizes
```

AQE obtains these statistics from execution.

Spark's documentation explicitly identifies runtime statistics as part
of AQE, and notes that inaccurate or missing statistics can hinder
optimal plan selection.

------------------------------------------------------------------------

# 5. The Complete Catalyst + AQE Architecture

``` text
┌──────────────────────────────────────────────────────────────┐
│                         PySpark / SQL                        │
└──────────────────────────────┬───────────────────────────────┘
                               |
                               v
┌──────────────────────────────────────────────────────────────┐
│                 Parsed / Unresolved Logical Plan             │
└──────────────────────────────┬───────────────────────────────┘
                               |
                               v
┌──────────────────────────────────────────────────────────────┐
│                         Analyzer                             │
│                                                              │
│  Resolve tables                                              │
│  Resolve columns                                             │
│  Resolve aliases                                             │
│  Resolve functions                                           │
│  Resolve data types                                          │
│  Resolve references                                          │
└──────────────────────────────┬───────────────────────────────┘
                               |
                               v
┌──────────────────────────────────────────────────────────────┐
│                    Resolved Logical Plan                     │
└──────────────────────────────┬───────────────────────────────┘
                               |
                               v
┌──────────────────────────────────────────────────────────────┐
│                  CATALYST OPTIMIZER                          │
│                                                              │
│  Predicate pushdown                                          │
│  Column pruning                                              │
│  Constant folding                                            │
│  Expression simplification                                   │
│  Boolean simplification                                      │
│  Filter combination                                          │
│  Projection simplification                                   │
│  Partition pruning                                           │
│  Join-related logical optimization                           │
│  Aggregate-related optimization                              │
│  Subquery optimization                                       │
│  Null propagation                                            │
│  Common subexpression elimination                             │
│  ...                                                         │
└──────────────────────────────┬───────────────────────────────┘
                               |
                               v
┌──────────────────────────────────────────────────────────────┐
│                    Optimized Logical Plan                    │
└──────────────────────────────┬───────────────────────────────┘
                               |
                               v
┌──────────────────────────────────────────────────────────────┐
│                    Physical Planner                          │
│                                                              │
│  Broadcast Hash Join                                        │
│  Sort Merge Join                                             │
│  Shuffled Hash Join                                          │
│  Broadcast Nested Loop Join                                 │
│  HashAggregate / SortAggregate                               │
│  Exchange / Shuffle                                          │
│  Sort                                                        │
└──────────────────────────────┬───────────────────────────────┘
                               |
                               v
┌──────────────────────────────────────────────────────────────┐
│                    Initial Physical Plan                     │
└──────────────────────────────┬───────────────────────────────┘
                               |
                               v
                    ┌──────────────────────┐
                    │       AQE            │
                    │ Adaptive Execution   │
                    └──────────┬───────────┘
                               |
                 execute query stage(s)
                               |
                               v
                    ┌──────────────────────┐
                    │ Runtime Statistics   │
                    │                      │
                    │ actual bytes         │
                    │ actual partitions    │
                    │ partition sizes      │
                    │ actual row counts    │
                    └──────────┬───────────┘
                               |
                               v
                    ┌──────────────────────┐
                    │ Adaptive Replanning  │
                    └──────────┬───────────┘
                               |
          +--------------------+--------------------+
          |                    |                    |
          v                    v                    v
   Coalesce shuffle     Change join strategy    Handle skew
      partitions
          |                    |                    |
          +--------------------+--------------------+
                               |
                               v
                     Continue execution
```

------------------------------------------------------------------------

# 6. What Exactly Is AQE?

**Adaptive Query Execution (AQE)** is a Spark SQL execution framework
that uses **runtime statistics** to re-optimize a query plan while the
query is running.

The official Spark documentation describes AQE as re-optimizing the
query plan in the middle of execution based on accurate runtime
statistics.

AQE has been enabled by default since Spark 3.2.0.

------------------------------------------------------------------------

# 7. AQE Configuration

The umbrella configuration is:

``` python
spark.conf.get("spark.sql.adaptive.enabled")
```

or:

``` python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

In modern Spark releases it is enabled by default.

Check:

``` python
print(spark.conf.get("spark.sql.adaptive.enabled"))
```

------------------------------------------------------------------------

# 8. What AQE Can Do --- Complete Practical List

The most important AQE capabilities are:

``` text
1. Coalesce post-shuffle partitions
2. Convert Sort Merge Join -> Broadcast Hash Join
3. Convert Sort Merge Join -> Shuffled Hash Join
4. Detect and optimize skewed joins
5. Split skewed shuffle partitions
6. Use local shuffle readers after certain plan changes
7. Use runtime statistics for adaptive decisions
8. Re-optimize parts of the plan at query-stage boundaries
9. Perform adaptive partition sizing
10. Perform adaptive runtime optimizer rules
11. Reuse/re-optimize runtime subquery/exchange-related structures
12. Support adaptive dynamic partition pruning scenarios
13. Apply adaptive optimizer rules after runtime statistics become available
```

The core documented AQE features are post-shuffle partition coalescing,
skew handling, Sort Merge Join -\> Broadcast Hash Join conversion, Sort
Merge Join -\> Shuffled Hash Join conversion, and related adaptive
runtime planning.

------------------------------------------------------------------------

# 9. Capability #1 --- Coalescing Post-Shuffle Partitions

This is one of the most important AQE features.

Suppose:

``` python
spark.conf.set("spark.sql.shuffle.partitions", 2000)
```

You run:

``` python
df.groupBy("department").count()
```

The shuffle may create 2000 output partitions.

But suppose the actual data is only:

``` text
2 GB
```

You might end up with many tiny partitions:

``` text
Partition 1 = 1 MB
Partition 2 = 0.5 MB
Partition 3 = 2 MB
...
Partition 2000 = 0.7 MB
```

That means many tasks.

AQE can inspect actual post-shuffle partition sizes and combine
contiguous small partitions.

Conceptually:

``` text
Before:

2000 shuffle partitions
     ↓
many tiny tasks

AQE

After:

perhaps 100-300 larger tasks
     ↓
less task scheduling overhead
```

The exact number depends on runtime statistics and configuration.

------------------------------------------------------------------------

# 10. Why Static Catalyst Cannot Always Solve This

Suppose you configure:

``` python
spark.conf.set("spark.sql.shuffle.partitions", "2000")
```

before execution.

Catalyst has to plan using information available before the shuffle
finishes.

It cannot know with certainty:

``` text
Partition 1 = 0.5 MB
Partition 2 = 300 MB
Partition 3 = 4 MB
...
```

until the shuffle actually happens.

AQE can.

Therefore:

``` text
Catalyst:
"Use 2000 shuffle partitions."

AQE:
"I now know the actual shuffle output. Many are tiny.
Let's coalesce them."
```

------------------------------------------------------------------------

# 11. Example --- Small Dataset with Large Shuffle Partition Setting

``` python
spark.conf.set("spark.sql.shuffle.partitions", "2000")

result = (
    df
    .groupBy("department")
    .count()
)

result.show()
```

If the dataset produces only a small amount of shuffle output, AQE can
reduce the number of downstream tasks.

This is particularly useful when a generic cluster configuration uses a
high shuffle partition count.

------------------------------------------------------------------------

# 12. AQE Configuration for Coalescing

Important settings include:

``` text
spark.sql.adaptive.coalescePartitions.enabled
spark.sql.adaptive.coalescePartitions.initialPartitionNum
spark.sql.adaptive.advisoryPartitionSizeInBytes
spark.sql.adaptive.coalescePartitions.minPartitionSize
spark.sql.adaptive.coalescePartitions.parallelismFirst
```

The official Spark documentation states that AQE can coalesce contiguous
post-shuffle partitions according to runtime map-output statistics.

------------------------------------------------------------------------

# 13. `advisoryPartitionSizeInBytes`

This gives AQE a target size for adaptive partitioning.

Example:

``` python
spark.conf.set(
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "64MB"
)
```

Conceptually:

``` text
Actual shuffle partitions:

10 MB
15 MB
20 MB
5 MB
100 MB
8 MB
12 MB

AQE can combine compatible small contiguous partitions
toward an appropriate target.
```

This is not a guarantee that every partition becomes exactly 64 MB.

------------------------------------------------------------------------

# 14. `parallelismFirst`

A subtle but important configuration.

Modern Spark has:

``` text
spark.sql.adaptive.coalescePartitions.parallelismFirst
```

When true, Spark prioritizes maintaining parallelism rather than
strictly targeting the advisory partition size.

This matters when you tune AQE.

A good interview statement:

> AQE partition coalescing balances task size against available
> parallelism; the exact behavior is controlled by AQE partition-sizing
> configurations.

------------------------------------------------------------------------

# 15. Capability #2 --- Sort Merge Join to Broadcast Hash Join

This is probably the most famous AQE example.

Suppose the initial plan is:

``` text
SortMergeJoin
```

because Spark thinks both sides are large.

But after an earlier stage finishes:

``` text
Left side = 500 GB
Right side = 5 MB
```

AQE can convert:

``` text
SortMergeJoin
```

to:

``` text
BroadcastHashJoin
```

when the runtime size satisfies the adaptive broadcast threshold.

------------------------------------------------------------------------

# 16. Why Catalyst Couldn't Choose Broadcast Initially

Consider:

``` python
large_df.join(
    small_df.filter(col("country") == "India"),
    "customer_id"
)
```

Before executing the filter, Spark might estimate:

``` text
small_df = 500 MB
```

Therefore:

``` text
SortMergeJoin
```

But after filtering:

``` text
country = India
```

actual size may become:

``` text
4 MB
```

Now broadcasting is much better.

Catalyst couldn't necessarily know the post-filter runtime size.

AQE can.

------------------------------------------------------------------------

# 17. Full Example --- Join Conversion

Initial:

``` text
Large table
500 GB

Small table
500 MB

Initial plan:
SortMergeJoin
```

Execution of the small side:

``` text
Filter
  ↓
Actual output = 5 MB
```

AQE observes:

``` text
5 MB < adaptive broadcast threshold
```

Then:

``` text
SortMergeJoin
       ↓
BroadcastHashJoin
```

------------------------------------------------------------------------

# 18. Why Broadcast Join Is Better Here

Sort Merge Join generally involves:

``` text
Shuffle
+
Sort
+
Merge
```

Broadcast Hash Join can instead:

``` text
Broadcast small table
+
Hash lookup on large side
```

So AQE may eliminate the need for sorting both sides and can use local
shuffle reading in applicable situations.

------------------------------------------------------------------------

# 19. AQE Broadcast Threshold

Important configuration:

``` text
spark.sql.adaptive.autoBroadcastJoinThreshold
```

This controls the adaptive broadcast decision.

For comparison:

``` text
spark.sql.autoBroadcastJoinThreshold
```

is used for ordinary planning.

Interview distinction:

``` text
Static planning:
spark.sql.autoBroadcastJoinThreshold

Adaptive planning:
spark.sql.adaptive.autoBroadcastJoinThreshold
```

The adaptive setting defaults to the normal broadcast threshold unless
changed.

------------------------------------------------------------------------

# 20. Capability #3 --- Local Shuffle Reader

Suppose the initial plan contains:

``` text
SortMergeJoin
```

and shuffle data has been generated.

AQE discovers:

``` text
One side is small enough to broadcast
```

and changes the join to:

``` text
BroadcastHashJoin
```

The original shuffle partitioning requirement may no longer be necessary
in the same way.

AQE can use:

``` text
LocalShuffleReader
```

to read shuffle data locally where applicable.

This can reduce network traffic.

------------------------------------------------------------------------

# 21. Why Local Shuffle Reader Matters

Without the optimization:

``` text
Executor A
   |
network
   |
Executor B
```

With local reading where possible:

``` text
Executor
   |
local shuffle block
```

Less network transfer can improve performance.

Configuration:

``` text
spark.sql.adaptive.localShuffleReader.enabled
```

------------------------------------------------------------------------

# 22. Capability #4 --- Sort Merge Join to Shuffled Hash Join

AQE can also convert a Sort Merge Join into a Shuffled Hash Join when
runtime post-shuffle partition sizes are suitable.

Initial:

``` text
SortMergeJoin
```

Runtime:

``` text
All post-shuffle partitions are sufficiently small
```

AQE:

``` text
ShuffledHashJoin
```

------------------------------------------------------------------------

# 23. Why Would Shuffled Hash Join Be Better?

Sort Merge Join requires:

``` text
Shuffle
+
Sort
+
Merge
```

If each post-shuffle partition is small enough to build a local hash map
efficiently, a Shuffled Hash Join may avoid sorting.

Conceptually:

``` text
SMJ:

Shuffle -> Sort -> Merge

SHJ:

Shuffle -> Build hash map -> Probe
```

AQE can make this decision using actual partition sizes.

------------------------------------------------------------------------

# 24. Important Configuration

``` text
spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold
```

This controls the maximum size per partition for the local hash map in
the adaptive shuffled-hash decision.

------------------------------------------------------------------------

# 25. Capability #5 --- Skew Join Optimization

This solves a major distributed-processing problem.

Suppose a join has:

``` text
Partition 1 = 100 MB
Partition 2 = 120 MB
Partition 3 = 110 MB
Partition 4 = 80 GB
```

Partition 4 is severely skewed.

Most tasks finish quickly:

``` text
Task 1 -> 30 seconds
Task 2 -> 35 seconds
Task 3 -> 32 seconds
```

But:

``` text
Task 4 -> 45 minutes
```

The entire stage may wait for Task 4.

This is a classic straggler problem.

------------------------------------------------------------------------

# 26. How AQE Handles Skew

AQE can identify a skewed partition based on:

``` text
partition size
+
median partition size
+
configured thresholds
```

A partition can be considered skewed when it exceeds both:

``` text
skewedPartitionFactor × median partition size
```

and:

``` text
skewedPartitionThresholdInBytes
```

The Spark documentation currently lists defaults such as a skew factor
of 5.0 and a skew threshold of 256 MB.

------------------------------------------------------------------------

# 27. Skew Example

Suppose:

``` text
Median partition size = 100 MB
```

and:

``` text
skewedPartitionFactor = 5
```

Then:

``` text
5 × 100 MB = 500 MB
```

A partition larger than 500 MB and also larger than the configured
absolute threshold may qualify as skewed.

Suppose:

``` text
Partition 10 = 20 GB
```

AQE can split the skewed partition into smaller pieces.

------------------------------------------------------------------------

# 28. Before AQE

``` text
                    Join
                      |
        +-------------+-------------+
        |             |             |
      100MB         120MB          20GB
        |             |             |
      Task 1        Task 2        Task 3
                                     |
                                  very slow
```

------------------------------------------------------------------------

# 29. After AQE Skew Handling

``` text
                    Join
                      |
        +-------------+-------------+
        |             |             |
      100MB         120MB        Skewed 20GB
        |             |              |
      Task 1        Task 2      +----+----+----+----+
                                |    |    |    |
                              5GB  5GB  5GB  5GB
```

Multiple tasks can process portions of the skewed data.

------------------------------------------------------------------------

# 30. Important AQE Skew Configurations

``` text
spark.sql.adaptive.skewJoin.enabled

spark.sql.adaptive.skewJoin.skewedPartitionFactor

spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes

spark.sql.adaptive.forceOptimizeSkewedJoin
```

The exact defaults can vary by Spark version.

------------------------------------------------------------------------

# 31. Capability #6 --- Optimize Skewed Rebalance Partitions

AQE is not limited to join skew.

Spark also has adaptive handling for skewed shuffle partitions created
by `RebalancePartitions`.

The relevant setting includes:

``` text
spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled
```

This allows skewed partitions to be split according to adaptive target
sizes.

------------------------------------------------------------------------

# 32. Capability #7 --- Runtime Statistics

This is not just a feature; it is the foundation of AQE.

Catalyst might have:

``` text
Estimated rows = 100 million
```

Actual execution:

``` text
Actual rows = 5 million
```

AQE can use:

``` text
5 million
```

for subsequent decisions.

------------------------------------------------------------------------

# 33. Estimated Statistics vs Runtime Statistics

### Static planning

``` text
Estimate:
Table = 100 GB
Rows = 1 billion
```

### Runtime

``` text
Actual:
Table = 3 GB
Rows = 25 million
```

AQE can make decisions from:

``` text
Actual = 3 GB
```

rather than continuing to rely on:

``` text
Estimated = 100 GB
```

------------------------------------------------------------------------

# 34. Why Estimates Can Be Wrong

Estimates can be inaccurate because of:

``` text
Missing statistics
Stale statistics
Data distribution changes
Correlated columns
Highly selective filters
Complex expressions
Data arriving from external sources
Poor cardinality estimates
Changing data volumes
```

Spark documentation explicitly notes that missing or inaccurate
statistics can prevent optimal plan selection.

------------------------------------------------------------------------

# 35. Capability #8 --- Adaptive Runtime Logical Optimization

AQE is not only about changing join operators.

Spark has an adaptive optimizer that can apply rules using runtime
statistics.

This is important conceptually:

``` text
Static Catalyst optimizer
        |
pre-execution knowledge

AQE optimizer
        |
runtime knowledge
```

Modern Spark also exposes mechanisms for injecting runtime optimizer
rules.

------------------------------------------------------------------------

# 36. Dynamic Partition Pruning and AQE

Dynamic Partition Pruning (DPP) is closely related to adaptive/runtime
optimization.

Consider:

``` text
Sales fact table:
year/month/date partitions

Small dimension:
selected customers
```

Query:

``` sql
SELECT ...
FROM sales s
JOIN customers c
ON s.customer_id = c.customer_id
WHERE c.region = 'South';
```

The dimension-side filter can determine which customer IDs are relevant.

Spark can use those runtime values to avoid scanning irrelevant
fact-table partitions.

------------------------------------------------------------------------

# 37. DPP Example

Fact:

``` text
sales/
    country=India/
    country=USA/
    country=UK/
    country=Australia/
```

Dimension filter:

``` text
country = India
```

Potentially:

``` text
Scan India
Skip USA
Skip UK
Skip Australia
```

This is particularly powerful for star-schema workloads.

------------------------------------------------------------------------

# 38. Important Nuance About DPP

Do not say:

> "DPP is entirely an AQE feature."

Dynamic Partition Pruning is a broader Spark SQL optimization feature
and has evolved across Spark releases. AQE can participate in
runtime/DPP scenarios, but DPP should not be treated as synonymous with
AQE.

------------------------------------------------------------------------

# 39. Capability #9 --- Adaptive Join Selection

AQE can make decisions such as:

``` text
Initial:
SortMergeJoin

Runtime:
BroadcastHashJoin
```

or:

``` text
Initial:
SortMergeJoin

Runtime:
ShuffledHashJoin
```

This means AQE can effectively answer:

> "Given what I now know, is the join strategy I initially chose still
> appropriate?"

------------------------------------------------------------------------

# 40. Capability #10 --- Adaptive Number of Reducer/Shuffle Tasks

One of the major motivations for AQE is that Spark can adapt the number
of post-shuffle tasks.

Static:

``` text
spark.sql.shuffle.partitions = 2000
```

Runtime:

``` text
Actual shuffle data = small
```

AQE:

``` text
Coalesce partitions
```

This is why modern Spark can be less sensitive to choosing one perfect
global shuffle-partition number.

------------------------------------------------------------------------

# 41. The Classic Problem AQE Solves

Without AQE, you often had to choose:

``` text
spark.sql.shuffle.partitions
```

too low:

``` text
100 partitions
```

for a huge dataset:

``` text
10 TB
```

could create large partitions and slow tasks.

Too high:

``` text
10,000 partitions
```

for a small dataset:

``` text
5 GB
```

could create many tiny tasks.

AQE helps dynamically adjust post-shuffle partitioning.

------------------------------------------------------------------------

# 42. But AQE Does Not Mean You Never Tune Partitions

This is an important interview nuance.

AQE reduces the need for perfect manual tuning, but:

``` text
Initial shuffle partition count
+
AQE settings
+
cluster resources
+
data volume
```

still matter.

A huge initial partition count can still create unnecessary overhead.

A very low initial count can also limit parallelism before AQE has an
opportunity to help.

------------------------------------------------------------------------

# 43. Catalyst's "Gap" #1 --- It Cannot Know Future Runtime Output Exactly

Example:

``` python
df.filter(col("country") == "India")
```

Before execution:

``` text
How many rows?
```

Maybe:

``` text
Estimated = 100 million
```

Actual:

``` text
1 million
```

Catalyst cannot reliably know the exact runtime result beforehand.

AQE sees it after the stage executes.

------------------------------------------------------------------------

# 44. Catalyst's "Gap" #2 --- Cardinality Estimation Errors

Suppose:

``` text
Estimated:
100 million rows

Actual:
1 million rows
```

Catalyst may choose a plan based on:

``` text
100 million
```

AQE can adapt after discovering:

``` text
1 million
```

This can dramatically change:

``` text
Join strategy
Partition count
Skew handling
Local reading strategy
```

------------------------------------------------------------------------

# 45. Catalyst's "Gap" #3 --- Data Distribution

Suppose total data is:

``` text
1 TB
```

That alone doesn't tell you distribution.

You might have:

``` text
Partition 1 = 50 MB
Partition 2 = 40 MB
Partition 3 = 60 MB
Partition 4 = 850 GB
...
```

Catalyst's static plan cannot fully observe actual shuffle distribution
before execution.

AQE can inspect actual partition sizes.

------------------------------------------------------------------------

# 46. Catalyst's "Gap" #4 --- Runtime Skew

A query can look reasonable before execution:

``` text
Table A = 1 TB
Table B = 1 TB
```

But after joining:

``` text
Key = UNKNOWN
```

might dominate:

``` text
90% of the rows
```

The resulting partition can be enormous.

AQE can detect skew after the shuffle.

------------------------------------------------------------------------

# 47. Catalyst's "Gap" #5 --- Runtime Join Size

Suppose:

``` text
Table B = 1 GB
```

before filtering.

Query:

``` python
B.filter(col("status") == "ACTIVE")
```

Actual:

``` text
B after filter = 2 MB
```

Static planning may not know that exact result.

AQE can.

------------------------------------------------------------------------

# 48. Catalyst's "Gap" #6 --- Actual Shuffle Size

Catalyst may plan:

``` text
Exchange
```

but it cannot know exactly how much data the exchange will produce until
execution.

AQE can inspect:

``` text
map output statistics
```

and make decisions from those actual sizes.

------------------------------------------------------------------------

# 49. Catalyst's "Gap" #7 --- Actual Partition Size

Static:

``` text
2000 partitions
```

Runtime:

``` text
most partitions = 2 MB
```

AQE:

``` text
coalesce them
```

This is a direct example of runtime knowledge filling a
static-information gap.

------------------------------------------------------------------------

# 50. Catalyst's "Gap" #8 --- Stragglers Caused by Skew

Catalyst can optimize logical expressions and choose an initial physical
strategy.

But it cannot know every future straggler before the shuffle occurs.

AQE can observe:

``` text
Partition 7 = 40 GB
```

while:

``` text
median = 100 MB
```

and react.

------------------------------------------------------------------------

# 51. Catalyst's "Gap" #9 --- Plan Decisions Become Better After Earlier Stages Finish

This is the core AQE concept.

Imagine:

``` text
Stage 1
Filter
   |
   v
Actual result known
   |
   v
Stage 2
Join
```

Catalyst initially had to choose the plan for Stage 2 without knowing
the exact result of Stage 1.

AQE waits for Stage 1 to finish and then can improve Stage 2.

------------------------------------------------------------------------

# 52. Query Stage Concept

AQE divides execution around materialization boundaries such as
exchanges.

Conceptually:

``` text
Stage 1
Filter
  |
  v
Shuffle / Exchange
  |
  v
Runtime statistics
  |
  v
AQE re-optimization
  |
  v
Stage 2
Join
```

The exchange gives Spark a useful point where actual statistics become
available.

------------------------------------------------------------------------

# 53. Why AQE Is "Adaptive"

Normal execution:

``` text
Plan
 ↓
Execute
 ↓
Done
```

AQE:

``` text
Plan
 ↓
Execute stage
 ↓
Observe
 ↓
Re-optimize
 ↓
Execute next stage
 ↓
Observe
 ↓
Re-optimize
 ↓
Continue
```

That feedback loop is the key.

------------------------------------------------------------------------

# 54. Static vs Adaptive --- Simple Example

## Without AQE

``` text
Estimated dimension = 300 MB

        ↓

SortMergeJoin

        ↓

Execute
```

## With AQE

``` text
Estimated dimension = 300 MB

        ↓

Initial SortMergeJoin

        ↓

Filter stage executes

        ↓

Actual dimension = 5 MB

        ↓

AQE

        ↓

BroadcastHashJoin

        ↓

Continue
```

------------------------------------------------------------------------

# 55. Another Example --- Partition Count

## Without AQE

``` text
shuffle.partitions = 2000

        ↓

2000 tasks
```

## With AQE

``` text
shuffle.partitions = 2000

        ↓

Runtime shuffle statistics

        ↓

Many partitions are tiny

        ↓

AQE coalesces

        ↓

Fewer downstream tasks
```

------------------------------------------------------------------------

# 56. Another Example --- Skew

## Without AQE

``` text
100 partitions

99 partitions:
~100 MB each

1 partition:
50 GB

        ↓

One very slow task
        ↓
Stage waits
```

## With AQE

``` text
Detect 50 GB skewed partition

        ↓

Split skewed partition

        ↓

Multiple tasks

        ↓

Less straggler impact
```

------------------------------------------------------------------------

# 57. Catalyst vs AQE --- Detailed Comparison

  -----------------------------------------------------------------------
  Area                    Catalyst / Static       AQE
                          Planning                
  ----------------------- ----------------------- -----------------------
  Main role               Analyze and optimize    Adapt execution using
                          query plan              runtime facts

  Timing                  Before execution        During execution

  Uses runtime statistics No, not in the same     Yes
                          adaptive sense          

  Logical optimization    Major responsibility    Can apply adaptive
                                                  runtime rules

  Physical strategy       Yes                     Can revise selected
  selection                                       strategy

  Join conversion after   No                      Yes
  runtime                                         

  Coalesce post-shuffle   No                      Yes
  partitions                                      

  Runtime skew detection  No                      Yes

  Split skewed join       No                      Yes
  partitions                                      

  Exact shuffle output    Unknown beforehand      Known after shuffle
  size                                            stage

  Exact post-filter size  Often estimated         Can observe actual
                                                  result

  Runtime partition sizes Unknown                 Available

  Handles stale estimates Limited                 Can compensate after
                                                  runtime facts arrive

  Broadcast conversion    No                      Yes
  based on runtime                                

  Shuffled hash           No                      Yes
  conversion based on                             
  runtime                                         

  Local shuffle reader    No                      Yes
  after adaptive                                  
  conversion                                      

  Initial logical         Yes                     Not its primary role
  rewrites                                        

  Predicate pushdown      Yes                     Not its primary purpose

  Column pruning          Yes                     Not its primary purpose

  Constant folding        Yes                     Not its primary purpose

  Expression              Yes                     Not its primary purpose
  simplification                                  

  Partition pruning       Yes                     AQE can participate in
                                                  runtime scenarios

  UDF internals           Generally opaque        Also opaque

  Guarantees no shuffle   No                      No

  Fixes bad application   No                      No
  logic                                           
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 58. The Most Important Conceptual Difference

``` text
Catalyst asks:

"Based on what I know BEFORE execution,
what is the best plan?"

AQE asks:

"Now that I know what ACTUALLY happened,
should I change the remaining plan?"
```

------------------------------------------------------------------------

# 59. What Catalyst Can Do That AQE Is Not Primarily Designed To Do

Catalyst is the broader query optimization framework.

Examples:

``` text
Resolve expressions
Resolve relations
Resolve columns
Constant folding
Predicate pushdown
Column pruning
Expression simplification
Boolean simplification
Null propagation
Filter combination
Projection simplification
Subquery rewriting
Logical join transformations
Logical aggregate transformations
```

AQE should not be thought of as a replacement for these.

------------------------------------------------------------------------

# 60. What AQE Can Do That Static Catalyst Cannot Reliably Do

The strongest examples are:

``` text
Actual shuffle partition sizes
Actual post-filter relation size
Runtime skew detection
Runtime join strategy changes
Runtime partition coalescing
Runtime local shuffle reading
Runtime statistics-based re-optimization
```

------------------------------------------------------------------------

# 61. Catalyst and AQE Are Complementary

Wrong mental model:

``` text
Catalyst OR AQE
```

Correct:

``` text
Catalyst
   +
Physical Planner
   +
AQE
   +
Execution Engine
```

They work together.

------------------------------------------------------------------------

# 62. Complete Example --- Fact + Dimension

Suppose:

``` text
sales = 2 TB
customers = 100 GB
```

Query:

``` python
result = (
    sales
    .join(
        customers.filter(col("country") == "India"),
        "customer_id"
    )
    .groupBy("product_id")
    .count()
)
```

------------------------------------------------------------------------

# 63. Initial Catalyst/Physical Planning

Spark might estimate:

``` text
customers after filter = 10 GB
```

Therefore:

``` text
SortMergeJoin
```

could be selected.

------------------------------------------------------------------------

# 64. Runtime

The filter executes.

Actual:

``` text
customers after filter = 5 MB
```

AQE sees:

``` text
5 MB
```

and can potentially change:

``` text
SortMergeJoin
```

to:

``` text
BroadcastHashJoin
```

------------------------------------------------------------------------

# 65. Runtime Plan Evolution

``` text
Initial:

Sales
  |
Shuffle
  |
SortMergeJoin
  |
GroupBy


AQE observes:

Filtered customers = 5 MB


Adaptive:

Sales
  |
BroadcastHashJoin
  |
GroupBy
```

Potentially avoiding expensive sorting/shuffle behavior associated with
the original join strategy.

------------------------------------------------------------------------

# 66. Complete Example --- Small Result After Aggregation

Suppose:

``` python
result = (
    df
    .groupBy("department")
    .agg(sum("salary").alias("total"))
)
```

Initial:

``` text
spark.sql.shuffle.partitions = 2000
```

Actual number of departments:

``` text
50
```

The post-shuffle output is tiny.

AQE can coalesce downstream shuffle partitions so that Spark does not
need thousands of tiny tasks.

------------------------------------------------------------------------

# 67. Complete Example --- Skewed Customer

Suppose:

``` text
customer_id = 100
```

appears:

``` text
500 million times
```

while most customer IDs appear:

``` text
1000 times
```

A join on:

``` python
sales.join(customers, "customer_id")
```

can create an enormous skewed partition.

AQE can identify the oversized partition and apply skew-join handling
where applicable.

------------------------------------------------------------------------

# 68. AQE and `explain()`

Run:

``` python
df.explain(True)
```

For AQE, the physical plan may appear with:

``` text
AdaptiveSparkPlan
```

Example conceptually:

``` text
== Physical Plan ==

AdaptiveSparkPlan isFinalPlan=false
+- ...
```

After execution, Spark can show an adaptive/final plan in the UI and
appropriate explain output.

------------------------------------------------------------------------

# 69. `isFinalPlan=false`

When you see:

``` text
AdaptiveSparkPlan isFinalPlan=false
```

it means Spark has an adaptive plan that may still change as runtime
information becomes available.

After execution, the final adaptive plan may be available.

------------------------------------------------------------------------

# 70. How to Inspect AQE in Spark UI

Use the Spark SQL UI / SQL tab.

Look for:

``` text
AdaptiveSparkPlan
Statistics(..., isRuntime=true)
```

Runtime statistics can show:

``` text
actual row counts
actual sizes
runtime plan changes
```

Spark documentation specifically recommends looking for
`Statistics(..., isRuntime=true)` in the SQL UI when inspecting runtime
statistics.

------------------------------------------------------------------------

# 71. Useful Commands for Investigation

``` python
df.explain(True)
```

``` python
df.explain("formatted")
```

``` python
df.explain("cost")
```

Check:

``` python
spark.conf.get("spark.sql.adaptive.enabled")
```

Check:

``` python
spark.conf.get(
    "spark.sql.adaptive.coalescePartitions.enabled"
)
```

Check:

``` python
spark.conf.get(
    "spark.sql.adaptive.skewJoin.enabled"
)
```

------------------------------------------------------------------------

# 72. AQE Configuration Cheat Sheet

``` text
spark.sql.adaptive.enabled
```

Master switch.

``` text
spark.sql.adaptive.coalescePartitions.enabled
```

Enable post-shuffle partition coalescing.

``` text
spark.sql.adaptive.coalescePartitions.initialPartitionNum
```

Initial number used for adaptive coalescing in applicable scenarios.

``` text
spark.sql.adaptive.advisoryPartitionSizeInBytes
```

Target/advisory adaptive partition size.

``` text
spark.sql.adaptive.coalescePartitions.minPartitionSize
```

Minimum partition size considered during coalescing.

``` text
spark.sql.adaptive.coalescePartitions.parallelismFirst
```

Controls whether maintaining parallelism is prioritized over the
advisory target.

``` text
spark.sql.adaptive.autoBroadcastJoinThreshold
```

Adaptive broadcast threshold.

``` text
spark.sql.adaptive.localShuffleReader.enabled
```

Allows local shuffle reading in applicable adaptive plans.

``` text
spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold
```

Controls conditions for adaptive shuffled-hash join selection.

``` text
spark.sql.adaptive.skewJoin.enabled
```

Enable adaptive skew join handling.

``` text
spark.sql.adaptive.skewJoin.skewedPartitionFactor
```

Relative skew threshold factor.

``` text
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes
```

Absolute skew threshold.

``` text
spark.sql.adaptive.forceOptimizeSkewedJoin
```

Can force skew optimization even if an additional shuffle may be
introduced.

``` text
spark.sql.adaptive.optimizer.excludedRules
```

Allows adaptive optimizer rules to be excluded.

``` text
spark.sql.adaptive.customCostEvaluatorClass
```

Allows a custom adaptive cost evaluator.

------------------------------------------------------------------------

# 73. AQE Does NOT Automatically Fix Everything

This is extremely important.

AQE does NOT automatically fix:

``` text
Bad joins written unnecessarily
collect() of huge data
Huge Python UDF overhead
Bad serialization patterns
Poor cluster sizing
Too much driver-side processing
Poor file layout
Too many tiny input files
Bad partitioning at source
Unnecessary columns if the query forces them through opaque logic
Bad data modeling
Incorrect business logic
Network bottlenecks outside Spark's control
```

AQE is powerful, but it is not magic.

------------------------------------------------------------------------

# 74. AQE vs Small Files Problem

Suppose:

``` text
1 million Parquet files
each = 10 KB
```

AQE does not magically merge those input files before reading.

This is an **input-file/layout problem**, not primarily a post-shuffle
AQE problem.

You need strategies such as:

``` text
Compaction
OPTIMIZE / file compaction where supported
Better write partitioning
Avoiding excessive small-file creation
```

------------------------------------------------------------------------

# 75. AQE vs Data Skew at the Source

AQE can help with **shuffle/join skew**.

But if your underlying data layout is badly partitioned or contains
extreme file-level imbalance, AQE isn't a universal solution.

You still need:

``` text
Better partitioning
Salting where appropriate
Data model changes
File compaction
```

depending on the problem.

------------------------------------------------------------------------

# 76. AQE vs Python UDF

Suppose:

``` python
@udf
def my_udf(x):
    ...
```

AQE doesn't suddenly understand the internal Python logic.

It cannot say:

``` text
"Ah, this UDF is equivalent to salary > 50000,
so I'll push it into Parquet."
```

The UDF remains opaque.

Prefer:

``` python
col("salary") > 50000
```

when possible.

------------------------------------------------------------------------

# 77. AQE vs Global Sort

Query:

``` python
df.orderBy("salary")
```

AQE cannot eliminate the fundamental need for a global ordering
operation.

It may optimize surrounding execution, but:

``` text
Global ORDER BY
```

still has substantial distributed-processing requirements.

------------------------------------------------------------------------

# 78. AQE vs `collect()`

Query:

``` python
df.collect()
```

AQE cannot turn:

``` text
500 GB -> Driver
```

into a safe operation.

The application design itself is the problem.

------------------------------------------------------------------------

# 79. AQE vs Excessive `repartition()`

Suppose you write:

``` python
df.repartition(10000)
```

unnecessarily.

AQE does not mean:

> "I can remove any repartition the developer wrote."

Explicit repartitioning changes the query's partitioning semantics and
can force a shuffle.

You should not rely on AQE to rescue unnecessary repartitions.

------------------------------------------------------------------------

# 80. AQE vs Caching

If you repeatedly execute:

``` python
expensive_df = ...
expensive_df.cache()
```

AQE doesn't replace caching.

Caching solves:

``` text
Repeated computation
```

AQE solves:

``` text
Runtime plan adaptation
```

They are different optimization mechanisms.

------------------------------------------------------------------------

# 81. AQE vs Broadcast Hint

Suppose you explicitly write:

``` python
from pyspark.sql.functions import broadcast

result = large_df.join(
    broadcast(small_df),
    "id"
)
```

You are providing a join strategy hint.

AQE may still optimize surrounding execution, but you should understand
that an explicit hint is different from AQE discovering at runtime that
broadcasting is beneficial.

------------------------------------------------------------------------

# 82. Important: AQE Does Not Mean "Always Broadcast"

Example:

``` text
Runtime small side = 500 MB
```

If:

``` text
adaptive broadcast threshold = 10 MB
```

AQE will not simply broadcast it.

AQE makes decisions according to applicable rules and configuration.

------------------------------------------------------------------------

# 83. Important: AQE Can Introduce Trade-offs

Example:

``` text
Skew optimization
```

may require extra work or shuffle.

The goal isn't:

``` text
Minimum number of operations
```

The goal is:

``` text
Better total execution performance
```

Spark's adaptive optimizer uses cost-based logic and runtime
information.

------------------------------------------------------------------------

# 84. AQE and Cost

Spark's adaptive framework can use a cost evaluator.

Modern Spark exposes:

``` text
spark.sql.adaptive.customCostEvaluatorClass
```

If not configured, Spark uses its default cost evaluator.

This reinforces an important idea:

> AQE is not simply a collection of if/else tricks; it is an adaptive
> optimization framework.

------------------------------------------------------------------------

# 85. AQE's Most Important Decision Points

Think of AQE as operating at these points:

``` text
                    Initial Plan
                         |
                         v
                  Execute Stage 1
                         |
                         v
               Runtime Statistics
                         |
            +------------+------------+
            |            |            |
            v            v            v
        Partition      Join         Skew
         sizes        sizes        detection
            |            |            |
            v            v            v
        Coalesce     Change join    Split/optimize
        partitions    strategy        skew
            \            |            /
             \           |           /
              +----------+----------+
                         |
                         v
                   Continue query
```

------------------------------------------------------------------------

# 86. Catalyst vs AQE --- Timeline View

## Catalyst

``` text
t0
|
| Parse
|
| Analyze
|
| Optimize
|
| Physical planning
|
v
Execute
```

## AQE

``` text
t0
|
| Initial planning
|
| Execute stage
|
|--------------------|
| Runtime statistics |
|--------------------|
|
| Re-optimize
|
| Execute next stage
|
|--------------------|
| Runtime statistics |
|--------------------|
|
| Re-optimize
|
v
Final execution
```

------------------------------------------------------------------------

# 87. The "Information Boundary" Mental Model

This is perhaps the best way to remember the difference.

``` text
                INFORMATION AVAILABLE
                        |
                        v
        +----------------------------------+
        | Before execution                 |
        |                                  |
        | Schema                            |
        | Metadata                         |
        | Catalog statistics               |
        | File statistics                  |
        | Hints                            |
        | Estimates                        |
        +----------------+-----------------+
                         |
                         v
                    CATALYST
                         |
                         v
                 Initial plan
                         |
                         v
        +----------------------------------+
        | After runtime stages             |
        |                                  |
        | Actual row counts                |
        | Actual bytes                     |
        | Actual shuffle sizes             |
        | Actual partition sizes           |
        | Actual skew                      |
        +----------------+-----------------+
                         |
                         v
                       AQE
                         |
                         v
                 Adaptive plan
```

This explains **why AQE exists**.

------------------------------------------------------------------------

# 88. Interview Question --- "Did Catalyst Fail?"

Question:

> What gaps did Catalyst leave that AQE fills?

Best answer:

> Catalyst did not "fail." Static optimization and adaptive optimization
> solve different problems. Catalyst has to make an initial plan using
> information available before execution. Some of the most important
> facts---actual post-filter cardinality, actual shuffle output size,
> actual partition-size distribution and runtime skew---are only known
> after stages execute. AQE uses those runtime facts to revise the
> physical plan.

That is much more accurate than saying:

> Catalyst was bad and AQE fixed it.

------------------------------------------------------------------------

# 89. Interview Question --- "Is AQE Part of Catalyst?"

The safest answer is:

> AQE is part of Spark SQL's adaptive query execution machinery and
> works closely with Catalyst's optimizer and physical planning
> infrastructure, but it is useful to distinguish the traditional static
> Catalyst optimization phase from AQE's runtime adaptive optimization
> phase.

In code and Spark internals, you will encounter classes and rules under
both Catalyst and adaptive execution packages.

------------------------------------------------------------------------

# 90. Interview Question --- "Does AQE Replace Catalyst?"

Answer:

> No. AQE complements Catalyst. Catalyst still performs analysis and
> extensive static logical optimization and participates in physical
> planning. AQE takes the resulting execution plan and can adapt parts
> of it using runtime statistics.

------------------------------------------------------------------------

# 91. Interview Question --- "Can AQE Change a Join Strategy?"

Yes.

The classic example:

``` text
SortMergeJoin
       ↓
runtime statistics
       ↓
BroadcastHashJoin
```

Another:

``` text
SortMergeJoin
       ↓
runtime partition sizes
       ↓
ShuffledHashJoin
```

------------------------------------------------------------------------

# 92. Interview Question --- "Can AQE Reduce Shuffle Partitions?"

Answer carefully:

> AQE can coalesce post-shuffle partitions based on runtime map-output
> statistics. It does not simply rewrite every shuffle into an arbitrary
> number of partitions; it adaptively combines compatible post-shuffle
> partitions according to its configuration and runtime information.

------------------------------------------------------------------------

# 93. Interview Question --- "Can AQE Solve Data Skew?"

Answer:

> AQE can detect and optimize certain skewed shuffle partitions,
> especially skewed joins, by splitting oversized partitions so that
> multiple tasks can process the skewed data. It is not a universal
> replacement for data-model or salting strategies.

------------------------------------------------------------------------

# 94. Interview Question --- "Can AQE Remove All Shuffles?"

No.

If the query requires:

``` text
GROUP BY
JOIN requiring redistribution
GLOBAL ORDER BY
DISTINCT
```

a shuffle may still be fundamentally required.

AQE optimizes the execution around these operations; it doesn't violate
their semantics.

------------------------------------------------------------------------

# 95. Interview Question --- "What If Statistics Are Wrong?"

Static:

``` text
Estimated = 1 GB
```

Runtime:

``` text
Actual = 5 MB
```

AQE can react.

This is one of the strongest benefits of AQE.

------------------------------------------------------------------------

# 96. Interview Question --- "What Happens If AQE Is Disabled?"

Set:

``` python
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "false"
)
```

Then Spark does not perform the adaptive runtime re-optimization
provided by AQE.

You still have:

``` text
Analyzer
Catalyst logical optimization
Physical planning
Execution
```

but without AQE's runtime adaptations.

------------------------------------------------------------------------

# 97. Side-by-Side Example

Query:

``` python
result = (
    fact
    .filter(col("date") >= "2026-01-01")
    .join(
        dim.filter(col("active") == True),
        "id"
    )
    .groupBy("category")
    .count()
)
```

------------------------------------------------------------------------

## Without AQE

``` text
Estimate fact after filter
        |
Estimate dimension after filter
        |
Choose join strategy
        |
Choose shuffle partition plan
        |
Execute
        |
No runtime re-optimization
```

------------------------------------------------------------------------

## With AQE

``` text
Estimate initial plan
        |
Choose initial join
        |
Execute filter/shuffle stage
        |
Observe actual sizes
        |
Is dimension now tiny?
        |
       YES
        |
Broadcast join
        |
Observe shuffle partition sizes
        |
Are many partitions tiny?
        |
       YES
        |
Coalesce partitions
        |
Observe skew
        |
Is one partition huge?
        |
       YES
        |
Optimize skew
        |
Continue
```

------------------------------------------------------------------------

# 98. What AQE Adds to the Spark Performance Toolbox

Before AQE, you often had to manually reason about:

``` text
shuffle.partitions
broadcast thresholds
data skew
join strategies
partition sizing
```

AQE doesn't eliminate these concepts, but it automates/adapts several
decisions.

Modern approach:

``` text
Give Spark reasonable starting configurations
+
enable AQE
+
inspect runtime behavior
+
tune only where necessary
```

------------------------------------------------------------------------

# 99. Practical PySpark Configuration Example

``` python
spark.conf.set("spark.sql.adaptive.enabled", "true")

spark.conf.set(
    "spark.sql.adaptive.coalescePartitions.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.adaptive.skewJoin.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.adaptive.localShuffleReader.enabled",
    "true"
)
```

Always verify your Spark version and platform defaults before applying
configuration globally.

------------------------------------------------------------------------

# 100. Practical Example --- Inspecting AQE

``` python
result = (
    employees
    .filter(col("salary") > 50000)
    .join(departments, "dept_id")
    .groupBy("department_name")
    .count()
)

result.explain(True)

result.count()

result.explain(True)
```

Also inspect the SQL tab in the Spark UI after execution.

------------------------------------------------------------------------

# 101. How to Prove AQE Changed the Plan

You can deliberately create a scenario where:

``` text
Initial plan:
SortMergeJoin
```

and the runtime relation becomes small enough for:

``` text
BroadcastHashJoin
```

Then inspect the final adaptive plan.

Look for:

``` text
AdaptiveSparkPlan
```

and physical operators such as:

``` text
BroadcastHashJoin
```

instead of the original:

``` text
SortMergeJoin
```

------------------------------------------------------------------------

# 102. AQE Plan Evolution Example

``` text
INITIAL PLAN

          SortMergeJoin
          /          \
       Sort          Sort
        |             |
     Shuffle       Shuffle


                 ↓

          Execute stage


                 ↓

RUNTIME STATISTICS

Right side = 3 MB


                 ↓

AQE


                 ↓

FINAL PLAN

       BroadcastHashJoin
          /        \
      Scan       Broadcast
```

This is the single most important AQE diagram to memorize.

------------------------------------------------------------------------

# 103. AQE Skew Evolution Example

``` text
INITIAL

Shuffle
  |
  +-- P1 = 100 MB
  +-- P2 = 110 MB
  +-- P3 = 90 MB
  +-- P4 = 30 GB


RUNTIME

AQE detects P4 as skewed


FINAL

Shuffle
  |
  +-- P1 = 100 MB
  +-- P2 = 110 MB
  +-- P3 = 90 MB
  +-- P4a
  +-- P4b
  +-- P4c
  +-- P4d
  +-- ...
```

------------------------------------------------------------------------

# 104. AQE Partition-Coalescing Evolution

``` text
INITIAL

2000 shuffle partitions

        ↓

RUNTIME

Most partitions are tiny

        ↓

AQE

Combine contiguous small partitions

        ↓

FINAL

Fewer, larger partitions
```

------------------------------------------------------------------------

# 105. AQE Shuffled Hash Join Evolution

``` text
INITIAL:

SortMergeJoin

        ↓

Runtime:

All post-shuffle partitions are sufficiently small

        ↓

AQE:

ShuffledHashJoin

        ↓

Avoid unnecessary sorting
```

------------------------------------------------------------------------

# 106. Catalyst + AQE + Tungsten

The three concepts can be memorized as:

``` text
CATALYST
"What is a better plan?"

AQE
"Now that I know runtime facts,
should I change the plan?"

TUNGSTEN / CODE GENERATION
"How can I execute the chosen plan efficiently?"
```

------------------------------------------------------------------------

# 107. Catalyst + AQE + Execution

``` text
             Query
               |
               v
          CATALYST
               |
       logical optimization
               |
               v
       Physical Planner
               |
        initial strategy
               |
               v
             AQE
               |
       runtime statistics
               |
       adaptive decisions
               |
               v
        Final physical plan
               |
               v
     Whole-stage code generation
               |
               v
          Executors
```

------------------------------------------------------------------------

# 108. The "Before vs During" Rule

Whenever you are unsure whether something belongs to Catalyst or AQE,
ask:

> **Can Spark know this accurately before executing the relevant
> stage?**

If yes:

``` text
Likely static planning / Catalyst / physical planning
```

If no, and the information becomes available only after a stage runs:

``` text
Likely AQE territory
```

Examples:

  Question                                          Static?       Runtime?
  ------------------------------------------ -------------- --------------
  What columns are needed?                              Yes   Not required
  Is `10 + 20` equal to 30?                             Yes             No
  Which column is referenced?                           Yes             No
  Actual shuffle partition size?                         No            Yes
  Actual post-filter size?                     Not reliably            Yes
  Runtime skew?                                Not reliably            Yes
  Convert SMJ to BHJ based on actual size?     Not reliably            Yes
  Coalesce tiny shuffle partitions?                      No            Yes

------------------------------------------------------------------------

# 109. One Critical Nuance --- Catalyst Also Uses Statistics

Do not say:

> "Catalyst has no statistics."

That is incorrect.

Spark's static optimizer and physical planner can use:

``` text
Data source statistics
Catalog statistics
Column statistics
Table statistics
```

The problem is that these statistics may be:

``` text
missing
stale
incomplete
inaccurate
unable to capture runtime filtering effects
```

AQE adds a fundamentally different source:

``` text
actual runtime statistics
```

------------------------------------------------------------------------

# 110. Static Statistics vs Runtime Statistics

``` text
STATIC STATISTICS

Collected before query
        |
        v
Catalyst / Physical Planner


RUNTIME STATISTICS

Produced during query
        |
        v
AQE
```

This is the cleanest comparison.

------------------------------------------------------------------------

# 111. Why Runtime Statistics Are More Trustworthy for Some Decisions

Suppose catalog says:

``` text
10 million rows
```

but a filter produces:

``` text
10,000 rows
```

The catalog statistic describes the original relation.

The runtime statistic describes:

``` text
the actual relation after the executed operation
```

For the next plan decision, the second fact can be much more useful.

------------------------------------------------------------------------

# 112. AQE Does Not Rebuild Everything From Scratch

Another common misconception:

> "AQE reruns the entire query optimizer after every task."

Not exactly.

AQE works with query stages and runtime statistics and adaptively
changes parts of the execution plan as stages complete.

Think:

``` text
Execute known stage
      ↓
collect runtime facts
      ↓
adapt remaining plan
      ↓
continue
```

not:

``` text
restart entire query
```

------------------------------------------------------------------------

# 113. Does AQE Increase Planning Overhead?

Yes, adaptive planning introduces some runtime planning overhead.

The reason to use it is that the execution savings can outweigh that
overhead, especially for complex queries with:

``` text
large joins
shuffles
skew
uncertain cardinalities
many stages
```

For very small queries, the benefits may be negligible.

------------------------------------------------------------------------

# 114. When AQE Helps the Most

AQE is especially valuable when:

``` text
Data volumes vary significantly
Statistics are imperfect
Filters are highly selective
Join sizes are uncertain
Shuffle sizes vary
Data is skewed
Generic cluster settings are used
Workloads are dynamic
```

------------------------------------------------------------------------

# 115. When AQE Helps Less

AQE may provide less benefit when:

``` text
Query is extremely simple
Data is already well understood
Statistics are already excellent
No meaningful shuffle occurs
No join strategy adaptation is possible
No skew exists
No large stage boundaries provide useful runtime information
```

------------------------------------------------------------------------

# 116. AQE Is Particularly Valuable in Data Engineering Pipelines

Example medallion pipeline:

``` text
Bronze
  |
Filter / clean
  |
Silver
  |
Join dimensions
  |
Aggregate
  |
Gold
```

The size of Silver after cleaning can vary dramatically from day to day.

AQE can adapt downstream execution based on the actual runtime result.

This is one reason AQE is useful in production ETL.

------------------------------------------------------------------------

# 117. Example --- Daily Data Volume Changes

Monday:

``` text
Input = 500 GB
```

Tuesday:

``` text
Input = 5 TB
```

Wednesday:

``` text
Input = 100 GB
```

A static:

``` text
spark.sql.shuffle.partitions = 2000
```

may not be ideal for every day.

AQE can adapt post-shuffle execution to actual runtime sizes.

------------------------------------------------------------------------

# 118. Example --- Highly Selective Day

Suppose:

``` text
Bronze = 1 TB
```

but:

``` python
df.filter(col("event_date") == "2026-08-30")
```

returns:

``` text
500 MB
```

Then a downstream join might become broadcastable or require far fewer
post-shuffle tasks than the original full-table size suggests.

AQE can exploit the runtime result.

------------------------------------------------------------------------

# 119. Catalyst vs AQE --- Responsibility Matrix

``` text
+--------------------------------------+-----------+---------+
| Capability                            | Catalyst  | AQE     |
+--------------------------------------+-----------+---------+
| Parse/analyze query                  | YES       | NO      |
| Resolve columns                      | YES       | NO      |
| Resolve functions                   | YES       | NO      |
| Constant folding                    | YES       | NO*     |
| Predicate pushdown                  | YES       | NO*     |
| Column pruning                      | YES       | NO*     |
| Expression simplification            | YES       | NO*     |
| Initial physical planning            | YES       | NO      |
| Runtime statistics                   | NO        | YES     |
| Runtime join conversion              | NO        | YES     |
| Post-shuffle coalescing              | NO        | YES     |
| Runtime skew detection               | NO        | YES     |
| Skew partition splitting             | NO        | YES     |
| Local shuffle reader adaptation      | NO        | YES     |
| Runtime adaptive optimizer rules     | NO        | YES     |
+--------------------------------------+-----------+---------+

*Not AQE's primary responsibility; Spark's adaptive optimizer can also contain
additional runtime rules.
```

------------------------------------------------------------------------

# 120. The 5 AQE Features You MUST Know for Interviews

If the interviewer asks:

> "What are the main features of AQE?"

Give these first:

### 1. Coalescing post-shuffle partitions

``` text
Many tiny partitions
        ↓
Fewer appropriately sized partitions
```

### 2. Sort Merge Join → Broadcast Hash Join

``` text
Runtime small side
        ↓
Broadcast
```

### 3. Sort Merge Join → Shuffled Hash Join

``` text
Runtime small post-shuffle partitions
        ↓
Shuffled Hash Join
```

### 4. Skew join optimization

``` text
Huge skewed partition
        ↓
Split
        ↓
Parallel processing
```

### 5. Runtime statistics / adaptive re-planning

``` text
Actual runtime facts
        ↓
Modify remaining execution plan
```

------------------------------------------------------------------------

# 121. The 5 Catalyst Features You MUST Know

### 1. Predicate Pushdown

``` text
Filter closer to source
```

### 2. Column Pruning

``` text
Read only required columns
```

### 3. Constant Folding

``` text
10 + 20 -> 30
```

### 4. Expression / Boolean Simplification

``` text
condition AND TRUE -> condition
```

### 5. Logical / physical planning

``` text
Choose an efficient execution strategy
```

------------------------------------------------------------------------

# 122. The Ultimate Comparison

``` text
                 CATALYST
                    |
                    | "I know the query."
                    |
                    v
          Static optimization
                    |
                    v
            Initial physical plan
                    |
                    v
                 EXECUTE
                    |
                    v
        "Now I know what happened."
                    |
                    v
                   AQE
                    |
                    +--------------------+
                    |                    |
                    v                    v
             Runtime statistics      Actual skew
                    |                    |
                    v                    v
             Change join             Split skew
                    |
                    v
            Coalesce partitions
                    |
                    v
             Better final plan
```

------------------------------------------------------------------------

# 123. A Very Strong Interview Answer

If asked:

> **"What is AQE and why was it introduced when Catalyst already
> existed?"**

Say:

> AQE is Spark SQL's runtime adaptive optimization mechanism. Catalyst
> performs extensive query analysis and static optimization before
> execution, but it must make many decisions using estimates and
> statistics that may be missing or inaccurate. It cannot know the exact
> output cardinality after a filter, the actual size of a shuffle, the
> actual size of post-shuffle partitions, or runtime skew before those
> stages execute. AQE addresses this gap by materializing query stages,
> collecting runtime statistics, and adapting the remaining physical
> plan. For example, a Sort Merge Join initially planned because a
> relation appears large can be converted to a Broadcast Hash Join when
> runtime statistics show that the relation is actually small. AQE can
> also coalesce small post-shuffle partitions, convert suitable joins to
> Shuffled Hash Join, and split skewed join partitions.

That is an excellent senior-level answer.

------------------------------------------------------------------------

# 124. A Shorter 30-Second Answer

> Catalyst optimizes Spark queries before execution using logical and
> physical planning rules. The limitation is that many important facts
> are only known after execution starts, such as actual post-filter
> sizes, shuffle partition sizes and data skew. AQE fills this gap by
> using runtime statistics to adapt the remaining physical plan. Its
> major features are post-shuffle partition coalescing, Sort Merge Join
> to Broadcast Hash Join conversion, Sort Merge Join to Shuffled Hash
> Join conversion, and skew join optimization.

------------------------------------------------------------------------

# 125. A 10-Second Mental Model

Remember:

``` text
CATALYST = BEFORE

AQE = DURING

TUNGSTEN/CODEGEN = EXECUTION EFFICIENCY
```

Or:

``` text
Catalyst:
"What should I do?"

AQE:
"Given what actually happened,
should I change what I'm going to do?"

Execution engine:
"How efficiently can I do it?"
```

------------------------------------------------------------------------

# 126. Final Master Diagram

``` text
                               QUERY
                                 |
                                 v
                     ┌─────────────────────┐
                     │      ANALYZER       │
                     │                     │
                     │ resolve names/types │
                     └──────────┬──────────┘
                                |
                                v
                     ┌─────────────────────┐
                     │ CATALYST OPTIMIZER  │
                     │                     │
                     │ Predicate Pushdown  │
                     │ Column Pruning      │
                     │ Constant Folding    │
                     │ Expression Rules    │
                     │ Filter Rules        │
                     │ Join Rules          │
                     │ Aggregate Rules     │
                     │ Partition Pruning   │
                     └──────────┬──────────┘
                                |
                                v
                     ┌─────────────────────┐
                     │ PHYSICAL PLANNER    │
                     │                     │
                     │ BHJ / SMJ / SHJ     │
                     │ Aggregate           │
                     │ Exchange            │
                     │ Sort                │
                     └──────────┬──────────┘
                                |
                                v
                     ┌─────────────────────┐
                     │ INITIAL PHYSICAL    │
                     │ PLAN                │
                     └──────────┬──────────┘
                                |
                                v
                     ╔═════════════════════╗
                     ║        AQE          ║
                     ║                     ║
                     ║ Runtime Statistics  ║
                     ║        ↓            ║
                     ║ Re-optimize         ║
                     ║        ↓            ║
                     ║ +----------------+  ║
                     ║ | Join changes   |  ║
                     ║ | Coalesce       |  ║
                     ║ | Skew handling  |  ║
                     ║ | Local reader   |  ║
                     ║ | Runtime rules  |  ║
                     ║ +----------------+  ║
                     ╚══════════╤══════════╝
                                |
                                v
                     ┌─────────────────────┐
                     │ FINAL PHYSICAL PLAN │
                     └──────────┬──────────┘
                                |
                                v
                     ┌─────────────────────┐
                     │ CODE GENERATION /   │
                     │ EXECUTION ENGINE    │
                     └──────────┬──────────┘
                                |
                                v
                           EXECUTORS
```

------------------------------------------------------------------------

# 127. Final Takeaway

The most important thing is **not** to memorize dozens of AQE
configuration names.

Understand this sequence:

``` text
1. Catalyst creates an optimized initial plan.

2. Physical planning selects an execution strategy.

3. Spark executes a query stage.

4. The stage produces actual runtime statistics.

5. AQE examines those statistics.

6. AQE may discover that the original plan is no longer ideal.

7. AQE changes the remaining physical plan.

8. Spark continues execution with the improved plan.
```

And the key reason is:

``` text
              BEFORE EXECUTION
                     |
                     v
                ESTIMATES
                     |
                  CATALYST
                     |
                     v
               INITIAL PLAN
                     |
                     v
                  EXECUTE
                     |
                     v
              ACTUAL FACTS
                     |
                     v
                    AQE
                     |
                     v
              ADAPTIVE PLAN
```

> **Catalyst optimizes with what Spark knows before execution. AQE
> optimizes with what Spark learns during execution.**

That is the single most important relationship between the two.
