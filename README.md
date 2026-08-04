# session_11_framework

Reusable Databricks ETL framework ที่ implement **Medallion Architecture** (Bronze → Silver → Gold) แบบ config-driven โดยใช้ Python `dataclass` เป็นแกนหลักของ pipeline, พร้อม Data Quality (DQ) check ในชั้น Silver

---

## Architecture Overview

```
Raw Files (CSV/JSON/...)
        │
        ▼
   ┌───────────┐   Auto Loader (streaming, trigger=availableNow)
   │  Bronze   │   + _load_dt, _load_dttm, _metadata columns
   └───────────┘
        │
        ▼
   ┌───────────┐   Data Quality checks:
   │  Silver   │   - invalid value check (regex by type)
   │           │   - key null check
   │           │   - duplicate check (row dup / key dup)
   └───────────┘   → bad records แยกไปอีกตาราง (*_bad_rec)
        │
        ▼
   ┌───────────┐   Aggregation / Join
   │   Gold    │   (business-ready table)
   └───────────┘
```

---

## Components

### 1. `BronzeLayer`

รับผิดชอบ ingest ข้อมูลดิบเข้าสู่ Bronze table ด้วย Auto Loader (Structured Streaming)

| Field | Type | รายละเอียด |
|---|---|---|
| `header` | `bool` | ไฟล์มี header row หรือไม่ |
| `delimiter` | `str` | ตัวคั่นคอลัมน์ |
| `file_path` | `str` | path ของไฟล์ต้นทาง |
| `table_name` | `str` | ชื่อ base table (ใช้ derive ชื่อ bronze table) |

**Methods:**
- `from_config_table(pipeline_name)` — classmethod อ่าน config จาก `session_11_framework.config_table` แล้วสร้าง instance
- `read_from_file()` — อ่านไฟล์ด้วย `readStream` (infer schema จาก batch read ก่อน), เติม metadata columns (`_load_dt`, `_load_dttm`, `_file_name`, `_file_path`, `_file_size`, `_file_mod`) แล้วเขียนลง Bronze table แบบ `outputMode("append")` พร้อม checkpoint


### 2. `SilverLayer`

รับผิดชอบ clean, validate, dedupe ข้อมูลจาก Bronze แล้วเขียนลง Silver พร้อมแยก bad records

| Field | Type | รายละเอียด |
|---|---|---|
| `table_name` | `str` | base table name |
| `schema_detail` | `dict[str,str]` | mapping ชื่อคอลัมน์ → ชนิดข้อมูล (ใช้ cast + validate) |
| `keys` | `list[str]` | primary key column(s) |
| `mode_type` | `str` | write mode ตอนเขียน Silver table |

**Data Quality pipeline:**
1. `read_from_bronze_sk()` — อ่าน Bronze เฉพาะ partition วันนี้ พร้อมสร้าง surrogate key (`_sk`) ด้วย `monotonically_increasing_id()`
2. `get_invalid_df()` — เช็ค format ผิด type ด้วย regex (เช่น `int`, `date`) ต่อคอลัมน์
3. `get_key_null_df()` — เช็คว่า key column เป็น null หรือไม่
4. `get_dup_df()` — แยกตรวจ 2 แบบ: `_row_dup` (ทุกคอลัมน์ซ้ำ) และ `_key_dup` (key ซ้ำแต่ข้อมูลไม่เหมือนกัน)
5. `union_all_bad_rec()` — รวม 3 ผลลัพธ์ข้างบน, group by แล้ว flatten `reason` เป็น array เดียวต่อ record
6. `get_final_result()` — filter record ที่ดีออกจาก bad records, cast type ตาม `schema_detail`, เติม `_load_dt` / `_load_dttm`
7. `load_bad_rec_to_table()` / `load_to_silver_table()` — เขียนผลลัพธ์ลงตาราง

**Helper function:** `get_reason(df)` — ใช้ `.melt()` แปลง flag columns (`_is_xxx_invalid`, `_is_xxx_Null`) ให้เป็น long format แล้วดึงเฉพาะ reason ที่เป็น `True` ออกมาเป็น array

### 3. Gold Layer (ตัวอย่าง script)

Join `fact_sales_silver` กับ `shop_name_silver`, aggregate ยอดขายรายร้าน แล้วเขียนลง `final_result_gold`

```python
sales_fact_df = (
    spark.table("session_11_framework.fact_sales_silver")
    .select("shop_id", "sales_qty")
    .filter(col("sales_date") == "2025-11-30")
)

shop_name_df = (
    spark.table("session_11_framework.shop_name_silver")
    .select("shop_id", "shop_name")
)

final_result_df = (
    sales_fact_df.alias("sales_fact")
    .join(
        shop_name_df.alias("shop_name"),
        col("sales_fact.shop_id") == col("shop_name.shop_id"),
        "left"
    )
    .select("sales_fact.shop_id", "sales_fact.sales_qty", "shop_name.shop_name")
    .groupBy("shop_name")
    .agg(sum("sales_qty").alias("total_sales"))
    .filter(col("shop_name").isNotNull())
    .select("shop_name", "total_sales")
)

final_result_df.write.mode("overwrite").saveAsTable("session_11_framework.final_result_gold")
```

---

## Config-Driven Design

ทั้ง `BronzeLayer` และ `SilverLayer` มี `from_config_table(pipeline_name)` สำหรับอ่าน metadata จากตาราง `session_11_framework.config_table` — ทำให้เพิ่ม pipeline ใหม่ได้โดยไม่ต้องแก้โค้ด แค่เพิ่ม row ใน config table

| ตารางที่ใช้ | คอลัมน์ที่ดึง |
|---|---|
| Bronze config | `header`, `delimiter`, `file_path`, `table_name` |
| Silver config | `table_name`, `schema_detail`, `keys`, `write_mode` |

---

## Naming Convention

| ตาราง | Pattern |
|---|---|
| Bronze | `{table_name}_bronze` |
| Silver | `{table_name}_silver` |
| Bad records | `{table_name}_bad_rec` |
| Gold | `{business_name}_gold` |


---

## Tech Stack

- **Databricks** (Unity Catalog)
- **PySpark** (Structured Streaming + Auto Loader)
- **Delta Lake**
