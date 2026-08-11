# PySpark Transformations Classification (Narrow vs Wide)

## Narrow Transformations
Narrow transformations do not require data shuffle across partitions.

### Common Narrow Transformations
- map()
- flatMap()
- filter()
- mapPartitions()
- mapPartitionsWithIndex()
- sample()
- union()
- coalesce() (without shuffle)
- distinct() (if no shuffle? usually wide but may depend)
- glom()
- pipe()
- mapValues() (RDD/PairRDD)
- flatMapValues()

## Wide Transformations
Wide transformations require data shuffle across partitions.

### Common Wide Transformations
- groupByKey()
- reduceByKey()
- aggregateByKey()
- combineByKey()
- join()
- leftOuterJoin()
- rightOuterJoin()
- fullOuterJoin()
- cogroup()
- repartition()
- sortByKey()
- sortBy()
- distinct() (generally treated as wide)
- intersection()
- subtractByKey()

## DataFrame / Dataset Transformations

### Narrow-like (no shuffle unless necessary)
- select()
- withColumn()
- filter() / where()
- drop()
- limit()

### Wide (shuffle operations)
- groupBy()
- join()
- orderBy()
- sort()
- distinct()
- dropDuplicates()

## Exceptions / Notes
- `coalesce(n)` -> Narrow transformation (no shuffle)
- `repartition(n)` -> Wide transformation (forces shuffle)
- `distinct()` -> Typically wide due to shuffle for uniqueness
- `sortByKey()` / `orderBy()` -> Always wide
- `groupByKey()` is expensive compared to reduceByKey (causes full shuffle)
- Some operations behave differently based on execution plan (optimizer may change behavior)

## Key Difference Summary
- Narrow: Operates within single partition
- Wide: Requires data movement (shuffle) across partitions

