# Spark Query Optimization: Predicate Pushdown, Column Pruning & Partition/File Pruning

## 1. Predicate Pushdown

**Definition:** Pushing filter conditions (`WHERE` clauses) down to the data source level so filtering happens *while reading* data, not after loading it all into memory.

**Example:**
```sql
SELECT * FROM orders WHERE order_date = '2024-01-01'
```
Instead of loading all rows then filtering, Spark instructs the reader to only return matching rows.

**How it works by source:**
- **Parquet/ORC:** Uses row-group/stripe-level min/max statistics. If a block's value range doesn't overlap the filter, the block is skipped entirely without decompression.
- **JDBC:** Filter is translated into SQL and pushed into the actual database query — reduces network I/O and DB load.
- **CSV/JSON:** No block-level pushdown possible. Every row must be parsed before Spark can apply the filter.

**Verify it in a plan:**
```scala
df.filter($"order_date" === "2024-01-01").explain(true)
// Look for "PushedFilters" in the physical plan
```

---

## 2. Column Pruning

**Definition:** Reading only the columns referenced in the query instead of the full row.

**Example:**
```sql
SELECT customer_id, amount FROM orders
```
Even if `orders` has 50 columns, only 2 are read from disk.

**Requirement:** Needs **columnar storage** (Parquet, ORC). Row-based formats (CSV, JSON, Avro) must read the entire row to extract any single field — pruning has no I/O benefit there.

---

## 3. Partition Pruning

**Definition:** Skipping entire directories/files based on partition column filters, done *before* any file is opened — a filesystem/metadata-level optimization.

**Example layout:**
```
/data/orders/year=2023/month=01/...
/data/orders/year=2024/month=01/...
```
```sql
SELECT * FROM orders WHERE year = 2024
```
Only the `year=2024` directory is listed and read.

**Requirement:** Table must be physically partitioned (Hive-style directories, or partition metadata in Delta/Iceberg). Works regardless of file format — it's about directory layout, not encoding.

**Caution:** Over-partitioning (too many small partitions/files) creates a "small file problem" that hurts performance more than it helps — driver spends excessive time listing files.

---

## 4. File Pruning / Data Skipping

**Definition:** Within a set of files, skipping individual files using metadata (min/max stats, bloom filters) without opening them.

- **Parquet/ORC:** Footer-level statistics per file/row-group enable skipping.
- **Delta Lake / Iceberg:** Maintain a transaction log/manifest with per-file column statistics — Spark prunes files by reading lightweight metadata, never touching the actual data files' footers. This is significantly faster at scale than opening thousands of Parquet footers.

---

## 5. Format Suitability Matrix

| Format | Predicate Pushdown | Column Pruning | Partition Pruning | File/Data Skipping |
|---|---|---|---|---|
| **Parquet** | ✅ Yes (row-group stats) | ✅ Yes (columnar) | ✅ Yes | ✅ Yes (strong) |
| **ORC** | ✅ Yes (stripe stats + bloom filters) | ✅ Yes (columnar) | ✅ Yes | ✅ Yes (strong) |
| **Avro** | ⚠️ Limited | ❌ No (row-based) | ✅ Yes (dir-based) | ❌ No |
| **CSV** | ❌ No | ❌ No | ✅ Yes (dir-based) | ❌ No |
| **JSON** | ❌ No | ❌ No | ✅ Yes (dir-based) | ❌ No |
| **Delta Lake** | ✅ Yes | ✅ Yes | ✅ Yes | ✅✅ Best (log-based) |
| **Iceberg** | ✅ Yes | ✅ Yes | ✅ Yes (hidden partitioning) | ✅✅ Best (manifest-based) |

**Key insight:** Partition pruning is format-agnostic (directory trick). Column pruning and block-level predicate pushdown require columnar, self-describing formats with embedded stats — this is why Parquet/ORC are the industry default, and Delta/Iceberg push it further with log/manifest-based metadata instead of per-file footer reads.

---

## 6. Bonus: Related Concepts Worth Knowing

- **Bloom Filters (ORC/Parquet):** Probabilistic structure that can quickly say "this value is *definitely not* in this block" — enables skipping even for high-cardinality equality filters where min/max ranges aren't selective enough.
- **Dynamic Partition Pruning (DPP):** Spark 3.0+ feature — in a join between a fact table and a filtered dimension table, Spark dynamically determines which partitions of the fact table to read based on the dimension table's filtered values, even though the filter isn't directly on the fact table.
- **Z-Ordering / Data Skipping (Delta Lake):** Co-locates related data within files across multiple columns so min/max-based file skipping works well even for multi-column filters, not just the partition column.
- **AQE (Adaptive Query Execution):** Handles what static pruning/pushdown can't — runtime skew, partition coalescing, and join strategy switching based on actual shuffle statistics (see Catalyst vs AQE discussion).
- **Hidden Partitioning (Iceberg):** Users query on a logical column (e.g., `event_date`) without knowing the physical partition transform (e.g., `day(event_timestamp)`) — avoids the common Hive mistake of forgetting to filter on the exact partition column.

---

## 7. Interview Questions & Answers

**Q1: What is predicate pushdown and why does it matter for performance?**
> It pushes filter conditions to the storage/data-source layer so unnecessary data is never read into Spark's memory. It reduces I/O, network transfer, and deserialization cost — the earlier data is eliminated, the cheaper the query.

**Q2: Why doesn't predicate pushdown work well with JSON or CSV?**
> These are row-based, uncompressed-structure formats without embedded block-level statistics. Spark must parse each row to evaluate a filter — there's no metadata to decide upfront which blocks to skip.

**Q3: What's the difference between partition pruning and file/data skipping?**
> Partition pruning eliminates entire directories based on partition column filters, before any file is touched — purely a folder-naming/metadata mechanism. File/data skipping goes a level deeper — within the remaining files, it uses column statistics (min/max, bloom filters) to skip individual files or row-groups that can't match the filter.

**Q4: Can column pruning work on a CSV file?**
> No — CSV is row-based, so Spark must read the full line to split it into fields, even if you only ask for one column. There's no I/O benefit; pruning only saves memory/CPU downstream, not disk read.

**Q5: Why is Parquet generally preferred over ORC or vice versa? Or over Avro?**
> Parquet and ORC are both columnar with min/max stats and support pushdown/pruning/column pruning. ORC has native bloom filter support baked in and is often favored in the Hive ecosystem. Avro is row-based, so it's better suited for write-heavy or streaming/schema-evolution use cases, not analytical read-heavy queries.

**Q6: What happens if you filter on a non-partition column in a partitioned table?**
> Partition pruning won't trigger — Spark still has to scan all partitions matching earlier conditions (or all partitions if none) and rely on predicate pushdown/file-level stats within those partitions instead.

**Q7: What is Dynamic Partition Pruning (DPP) and when does it kick in?**
> DPP (Spark 3.0+) applies in join queries where a large fact table is joined with a filtered, smaller dimension table. Spark computes the filtered join keys from the dimension side first, then uses them to prune fact table partitions dynamically at runtime — even though the fact table itself has no direct filter.

**Q8: How do Delta Lake/Iceberg improve on plain Parquet for pruning?**
> They maintain a transaction log/manifest with file-level statistics. Spark can decide which files to skip by reading this lightweight metadata rather than opening every Parquet file's footer individually — this matters enormously at scale (thousands/millions of files).

**Q9: If a table isn't partitioned, is pruning completely impossible?**
> Partition pruning specifically, yes. But file-level skipping (via min/max stats in Parquet/ORC footers, or Delta/Iceberg manifests) can still eliminate irrelevant files even without partitioning — just less efficiently, since Spark may need to inspect more metadata.

**Q10: How would you diagnose whether pushdown/pruning is actually happening?**
> Run `.explain(true)` or check the Spark UI's SQL tab — look for `PushedFilters` in the physical plan (for pushdown) and compare "files read" vs "files pruned" metrics for partition/file pruning. If a filter that should be pushed isn't showing in `PushedFilters`, it usually means the data source or filter expression type doesn't support it (e.g., certain UDFs or complex expressions block pushdown).

**Q11: Does predicate pushdown always guarantee correctness, or can it filter incorrectly?**
> Pushdown must be semantically equivalent — Spark only pushes filters the data source can evaluate correctly (this is why some UDFs or non-deterministic functions aren't pushed). If a source can't reliably apply a filter, Spark falls back to filtering in-memory after reading, so correctness isn't sacrificed, only potential performance.

**Q12: What's a common partitioning mistake in real-world pipelines?**
> Over-partitioning by high-cardinality columns (e.g., partitioning by `user_id`) leads to millions of tiny files/directories, causing driver-side listing overhead to dominate over any pruning benefit. Choosing low-cardinality, query-aligned partition columns (like `date`) is the usual best practice.
