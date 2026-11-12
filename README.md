# 1. project_framework

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


# 2. Unit Test Gold Layer Transform: สรุปยอดขายรายวัน (PySpark)

โปรเจกต์นี้เป็นฟังก์ชัน **Transformation ของชั้น Gold** ในสถาปัตยกรรม Medallion เขียนด้วย PySpark
ทำหน้าที่นำตารางข้อเท็จจริงยอดขาย (`sales_transaction`) มาเชื่อมกับตารางมิติร้านค้า (`shop_dimension`)
แล้วสรุปผลเป็นรายวัน พร้อม Unit Test ที่ใช้ `unittest` สำหรับตรวจสอบความถูกต้องของผลลัพธ์

[English version](README.md)

---

## สารบัญ

- [ภาพรวม](#ภาพรวม)
- [โครงสร้างโปรเจกต์](#โครงสร้างโปรเจกต์)
- [รายละเอียดฟังก์ชัน transform](#รายละเอียดฟังก์ชัน-transform)
- [ตัวอย่าง Input / Output](#ตัวอย่าง-input--output)
- [การทำงานของ Unit Test](#การทำงานของ-unit-test)
- [วิธีรันเทส](#วิธีรันเทส)

---

## ภาพรวม

```
sales_transaction_df ──┐
                       ├─► LEFT JOIN (shop_id) ─► GROUP BY sales_date ─► ตารางสรุปรายวัน
shop_dimension_df ─────┘
```

ฟังก์ชัน `transform` รับ DataFrame 2 ตัว และคืนค่า DataFrame ที่สรุปยอดขายต่อวัน
เป็นฟังก์ชันบริสุทธิ์ (ไม่อ่าน/เขียนไฟล์เอง) ทำให้เทสง่ายและนำไปใช้ซ้ำใน Pipeline หรือ Databricks Notebook ได้

## โครงสร้างโปรเจกต์

> test_transform_gold

```
.
├── logic_packages/
│   └── gold.py            # ฟังก์ชัน transform
└── tests/
    └── test_gold.py       # Unit test
```

```

---

## รายละเอียดฟังก์ชัน `transform`

```python
def transform(sales_transaction_df: DataFrame,
              shop_dimension_df: DataFrame) -> DataFrame:
```

| พารามิเตอร์ | ชนิด | คำอธิบาย |
|---|---|---|
| `sales_transaction_df` | `DataFrame` | ตารางรายการขาย |
| `shop_dimension_df` | `DataFrame` | ตารางมิติร้านค้า |

### ขั้นตอนการทำงาน

1. **Join:** `sales_transaction_df.join(shop_dimension_df, ['shop_id'], "left")`
   เชื่อมด้วยคอลัมน์ `shop_id` แบบ left join เพื่อให้ทุกรายการขายยังอยู่ครบ แม้ไม่พบร้านในตารางมิติ
   การใช้ `['shop_id']` (รายชื่อคอลัมน์) ทำให้ผลลัพธ์มีคอลัมน์ `shop_id` เพียงคอลัมน์เดียว ไม่ซ้ำกัน
2. **Group:** `.groupBy("sales_date")` จัดกลุ่มตามวันที่ขาย
3. **Aggregate:** คำนวณ 4 ค่าต่อวัน

| คอลัมน์ผลลัพธ์ | สูตร | ความหมาย |
|---|---|---|
| `sales_date` | - | วันที่ขาย (คีย์ของการ group) |
| `sum_sales` | `floor(sum(sales_amt))` | ยอดขายรวม ปัดเศษลง |
| `avg_sales` | `floor(avg(sales_amt))` | ยอดขายเฉลี่ยต่อรายการ ปัดเศษลง |
| `bucket_count` | `count(sales_amt)` | จำนวนรายการขาย (ไม่นับค่า `NULL`) |
| `max_sales` | `max(sales_amt)` | ยอดขายสูงสุดของรายการเดียวในวันนั้น |

>
---

## ตัวอย่าง Input / Output

**Input: ตารางยอดขาย**

| transaction_id | shop_id | sales_qty | sales_amt | sales_date |
|---|---|---|---|---|
| 1 | 1 | 88 | 5000.02 | 2025-01-01 |
| 2 | 1 | 94 | 318.44 | 2025-01-01 |
| 2 | 3 | 32 | 134.48 | 2025-01-01 |
| 3 | 2 | 25 | 8001.32 | 2025-12-01 |
| 4 | 2 | 7 | 205.12 | 2025-12-01 |
| 5 | 2 | 98 | 4959.73 | 2025-12-31 |
| 6 | 2 | 100 | 6000.25 | 2025-12-31 |

**Output: สรุปรายวัน**

| sales_date | sum_sales | avg_sales | bucket_count | max_sales |
|---|---|---|---|---|
| 2025-01-01 | 5452 | 1817 | 3 | 5000.02 |
| 2025-12-01 | 8206 | 4103 | 2 | 8001.32 |
| 2025-12-31 | 10959 | 5479 | 2 | 6000.25 |

ตัวอย่างการคำนวณของวันที่ 2025-01-01:
ผลรวม = 5000.02 + 318.44 + 134.48 = 5452.94 → `floor` ได้ **5452**
ค่าเฉลี่ย = 5452.94 / 3 ≈ 1817.65 → `floor` ได้ **1817**

---

## การทำงานของ Unit Test

ไฟล์เทสใช้ `unittest` โดยมีส่วนประกอบดังนี้

### 1. การ import แบบมี Fallback

```python
try:
    from logic_packages.gold import transform
except ImportError:
    ...  # เพิ่ม project_root เข้า sys.path แล้ว import ใหม่
```

พยายาม import ตรง ๆ ก่อน (เช่นรันบน Databricks หรือติดตั้งเป็นแพ็กเกจแล้ว)
ถ้าไม่ได้ จะหา `project_root` จากตำแหน่งไฟล์เทส แล้วเพิ่มเข้า `sys.path` เพื่อให้ import ได้เมื่อรันบนเครื่อง local

### 2. `setUpClass` / `tearDownClass`

- `setUpClass`: สร้าง `SparkSession` แบบ `local[1]` (ใช้ 1 core) ถ้าสร้างไม่สำเร็จ
  (เช่นรันใน Databricks ที่มีตัวแปร `spark` อยู่แล้ว) จะ fallback ไปใช้ `spark` ที่มีอยู่
- `tearDownClass`: หยุด SparkSession หลังเทสทั้งคลาสจบ

### 3. `test_apply_function`

1. สร้าง `fact_sales_df` และ `shop_df` จากข้อมูลจำลองพร้อม Schema ที่กำหนดเอง
2. กำหนด `expected_schema` และ `expected_data` ซึ่งเป็นผลลัพธ์ที่คาดหวัง
3. เรียก `transform(...)` เพื่อได้ผลลัพธ์จริง
4. **เรียงลำดับด้วย `sales_date` ทั้งสองฝั่ง** ก่อน `collect()` เพราะ `groupBy` ไม่รับประกันลำดับแถว
5. เปรียบเทียบด้วย `assertEqual` ว่าผลจริงตรงกับที่คาดหวัง

---

## วิธีรันเทส

จากโฟลเดอร์รากของโปรเจกต์:

```bash
# แบบ unittest
python -m unittest discover -s tests -v

# หรือรันไฟล์เดียว
python tests/test_gold.py
```

บน Databricks สามารถรันไฟล์เทสใน Notebook ได้โดยตรง
(โค้ดจะใช้ `spark` ที่มีอยู่แล้วผ่านกลไก fallback)

---


