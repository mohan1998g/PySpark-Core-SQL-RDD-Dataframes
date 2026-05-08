Below is a **TCS‑style SCD Type‑2 WHITEBOARD QUESTION SET**, exactly matching **how TCS interviews are conducted** (clear logic, step‑wise explanation, simple diagrams, no over‑engineering).

These are **realistic questions** you’ll be asked to **explain or solve on the whiteboard**, along with **model answers** you should give verbally.

***

# ✅ TCS SCD Type‑2 Whiteboard Questions (With Ideal Answers)

***

## ✅ Q1. What is SCD Type‑2? Explain with an example.

### ✅ Ideal Answer

> “SCD Type‑2 tracks full history of dimension attribute changes by inserting a new record for every change and expiring the old record using effective start and end dates.”

### ✅ Example (Explain on Whiteboard)

**Before**

    cust_id | city  | start_dt | end_dt     | is_current
    1       | Delhi | 2023-01-01 | 9999-12-31 | Y

**After city changes to Mumbai**

    cust_id | city   | start_dt | end_dt     | is_current
    1       | Delhi  | 2023-01-01 | 2024-01-10 | N
    1       | Mumbai | 2024-01-10 | 9999-12-31 | Y

✅ Key words to say:

*   “Expire old record”
*   “Insert new record”
*   “Maintain history”

***

## ✅ Q2. How do you identify whether a record has changed?

### ✅ Ideal Answer

> “I compare source and current dimension records using business keys and attribute values. If any relevant attribute differs, it is treated as a change.”

### ✅ Whiteboard Logic

    IF src.key NOT IN target → NEW INSERT
    ELSE IF src.attr != tgt.attr → CHANGE
    ELSE → NO ACTION

✅ Mention:

*   Column‑by‑column comparison **OR**
*   Hash comparison (for performance)

***

## ✅ Q3. What columns are mandatory in SCD Type‑2?

✅ **Expected Answer**

*   Business key (cust\_id)
*   Attribute columns
*   start\_date
*   end\_date
*   is\_current flag

✅ Optional (bonus points):

*   version
*   surrogate\_key

***

## ✅ Q4. How do you ensure only ONE active record per key?

✅ **Correct Answer**

> “By expiring the old record before inserting the new one and always filtering active records using `is_current = 'Y'`.”

✅ Whiteboard constraint:

    FOR each business_key:
      COUNT(is_current='Y') = 1

***

## ✅ Q5. How do you handle first‑time (initial) load?

✅ **Expected Answer**

> “For initial load, I insert all records with start\_date = load date, end\_date = 9999‑12‑31, and is\_current = 'Y'. No expiry logic is needed.”

***

## ✅ Q6. What happens if the source record did NOT change?

✅ **Expected Answer**

> “No action is taken. The existing active dimension record remains unchanged.”

✅ Whiteboard:

    src.attr == tgt.attr → SKIP

***

## ✅ Q7. Explain the full SCD2 flow end‑to‑end (most important)

✅ **Say this step‑by‑step**

1.  Read source data
2.  Deduplicate source (latest per key)
3.  Read current dimension records
4.  Join source with dimension
5.  Detect new & changed records
6.  Expire old records
7.  Insert new records
8.  Preserve unchanged records

✅ This explanation alone can clear the interview.

***

## ✅ Q8. How do you detect changes efficiently in large tables?

✅ **Best Answer**

> “I use hash‑based comparison instead of checking individual columns.”

✅ Whiteboard:

    src_hash = hash(col1, col2, col3)
    tgt_hash = hash(col1, col2, col3)

    IF src_hash != tgt_hash → CHANGE

***

## ✅ Q9. What is the difference between SCD Type‑1 and Type‑2?

✅ **Expected Answer**

| Type   | Behavior            |
| ------ | ------------------- |
| Type‑1 | Overwrites old data |
| Type‑2 | Preserves history   |

✅ Line to say:

> “Type‑1 loses history, Type‑2 maintains audit trail.”

***

## ✅ Q10. Can SCD Type‑2 cause data duplication?

✅ **Correct Answer**

> “Yes, on purpose. Multiple rows per business key are expected, but only one row is active.”

✅ Important phrase:

> “Historical rows ≠ duplicate data”

***

## ✅ Q11. How do you handle late‑arriving data?

✅ **Advanced Answer (Optional)**

> “Effective dates should be based on business event date, not load date. Late records may update historical rows instead of the latest one.”

✅ Mention only if asked further.

***

## ✅ Q12. How do you query the CURRENT record?

✅ **Answer**

```sql
WHERE is_current = 'Y'
```

***

## ✅ Q13. How do you get record as of a past date?

✅ **Answer**

```sql
WHERE '2023-06-01' BETWEEN start_dt AND end_dt
```

✅ This shows strong SCD understanding.

***

## ✅ Q14. What joins are used in SCD2?

✅ **Expected Answer**

*   Left join (source → target)
*   Sometimes broadcast join

✅ Line:

> “I join source with only active records.”

***

## ✅ Q15. How do you avoid multiple updates per key in one batch?

✅ **Correct Answer**

> “Deduplicate source using row\_number over business key ordered by update timestamp.”

✅ Mention window function (very good sign).

***

## ✅ Q16. Why is PySpark preferred over pure SQL for SCD2?

✅ **TCS‑Friendly Answer**

> “PySpark scales better, handles large datasets, supports CDC and streaming, and integrates well with data lakes.”

***

## ✅ Q17. What are common mistakes in SCD2?

✅ **Say 3–4 points**

*   Not expiring old records
*   Multiple active rows
*   Missing effective dates
*   Comparing NULLs incorrectly

***

## ✅ Q18. Explain SCD2 in one sentence (FINAL QUESTION)

✅ **Perfect One‑Liner**

> “SCD Type‑2 preserves full historical changes by expiring old dimension records and inserting new versions with effective date tracking.”

***

# ✅ How to Answer on Whiteboard (TCS Expectations)

✅ Draw simple tables  
✅ Use arrows (OLD → NEW)  
✅ Speak slowly  
✅ No complex Spark code unless asked  
✅ Explain logic first, code later

***

# ✅ Final TCS Interview TIP

If you confidently explain **Q7 (full flow)** and **Q1 (definition with example)**,  
👉 **you will clear the SCD interview round.**

***

Just tell me 👍
