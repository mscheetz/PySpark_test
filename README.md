# PySpark_Test

Learning project demonstrating Apache Spark with Python (PySpark). Three example scripts cover basic DataFrame usage, a more advanced sales-analytics workload, and real-world enterprise workload using both DataFrame API and Spark SQL.

## Overview

- **`simple.py`** - Minimal PySpark example. Creates a small in-memory DataFrame, computes a sum, and prints the rows.
- **`advanced.py`** - Sales analytics pipeline. Generates 1M synthetic sales records (or loads them from a cached Parquet dataset in `sample_sales/`), then runs aggregation queries via the DataFrame API and Spark SQL. Uses Hive support for table registration.
- **`multi.py`** - Enterprise analytics example. Creates four datasets representing customers (CRM), products (catalog), orders (ERP), and web events (clickstream), stores them in multiple formats (CSV, JSON, Parquet), then loads them into Spark to perform cross-source SQL joins, aggregations, customer engagement analysis, and revenue reporting.

## Quick start
   Ensure Java is installed (Spark requires Java 8/11/17).   

   ```bash
   java -version
   ```

   Java Install:
   ```bash
   sudo apt update
   sudo apt install openjdk-17-jdk
   ```

   Setup virtual environment:
   ```bash
   python3 -m venv env
   source env/bin/activate
   pip install -r requirements.txt
   source env/bin/activate
   ```

## Running the Examples

Simple sample
```bash
python src/simple.py
```

Advanced sample
```bash
python src/advanced.py
```

Multi-Data Types sample
```bash
python src/multi.py
```

## Tests

```bash
pytest -v
```
