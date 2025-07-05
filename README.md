# 🚖 End-to-End NYC Taxi Data Pipeline with Apache Spark and Delta Lake

This project implements a scalable, end-to-end data pipeline for processing and analyzing New York City (NYC) taxi trip data using **Apache Spark** and **Delta Lake**. The pipeline ingests raw data, applies transformations with performance optimizations, stores results efficiently, and demonstrates advanced features like **Change Data Feed (CDF)** and **time travel**.

---

## 📚 Table of Contents

- Project Goals  
- Technologies Used  
- Architecture & Workflow  
- File Structure & Descriptions  
- Optimizations Implemented  
- Setup Instructions  
- Usage  
- Challenges & Solutions  
- Future Work  
- Contributing  
- License  
- Contact

---

## 🎯 Project Goals

- **Ingest and Process Data**: Load raw NYC taxi trip data and perform transformations.  
- **Optimize Performance**: Apply Spark and Delta Lake optimizations for scalability and efficiency.  
- **Demonstrate Advanced Features**: Leverage Delta Lake's capabilities like CDF, time travel, and schema evolution.  
- **Automate Workflows**: Build a foundation for future automation (e.g., Airflow integration).

---

## 🛠 Technologies Used

- **Apache Spark**: Distributed data processing with PySpark (v3.5.0)  
- **Delta Lake**: Optimized storage layer with ACID transactions (v3.2.0)  
- **Python**: Core programming language (v3.10)  
- **Git**: Version control  

---

## 🏗 Architecture & Workflow

The pipeline follows a modular design with distinct stages:

1. **Ingestion**: Raw data (Parquet and CSV) is ingested into Delta Lake tables  
2. **Exploration**: Initial validation and exploration of ingested data  
3. **Transformation**: Cleaning, enrichment, and aggregation  
4. **Optimization**: Z-ordering, compaction, and partitioning  
5. **Analysis**: Advanced queries and performance testing  

Each stage is implemented via scripts in the `src/` directory, executed sequentially.

---

## 📁 File Structure & Descriptions

<code>
data/
├── delta/                        # Delta tables
├── intermediate/                # Lookup files, e.g., taxi_zone_lookup.csv
├── yellow_taxi_2023_01/         # Raw January trip data
├── yellow_taxi_2023_01_transformed/ # Transformed data output
├── yellow_tripdata_2023-01.parquet

spark_env/
└── spark_env_310/               # Virtual environment (Python 3.10)

spark-warehouse/                 # Spark SQL metadata and tables

src/
├── 01_ingest.py
├── 02_explore_delta.py
├── 03_analyze.py
├── 03_explore_transformed.py
├── 04_optimize.py
├── 05_test_performance.py
├── 06_test_partitioned.py
├── 07_vacuum.py
├── 08_enable_cdf.py
├── 09_test_cdf.py
├── 10_advanced_analysis.py
├── spark_script.py
├── test_env.py
└── verify_delta.py

.gitignore                       # Ignore files for Git
requirements.txt                 # Python dependencies
</code>

---

## ⚡ Optimizations Implemented

- **Caching**: In-memory caching of hot DataFrames  
- **Partitioning**: By `pickup_date` for efficient filtering  
- **Z-Ordering**: On `pickup_location_id`, `pickup_date` for skipping  
- **Broadcast Joins**: Small dimension tables avoid shuffles  
- **File Compaction**: `OPTIMIZE` command reduces small file overhead  

These ensure high performance even at scale.

---

## ⚙️ Setup Instructions

### 1. Clone the Repository

<code>
git clone https://github.com/udaykotian/Project-Spark-Delta-NYC-Taxi-Data-Pipeline-v2.git
cd Project-Spark-Delta-NYC-Taxi-Data-Pipeline-v2
</code>

### 2. Set Up the Environment

<code>
python3 -m venv spark_env_310
source spark_env_310/bin/activate
pip install -r requirements.txt
</code>

### 3. Download NYC Taxi Data

- Download `yellow_tripdata_2023-01.parquet` and `taxi_zone_lookup.csv` from the NYC TLC website  
- Place both files in the `data/` directory  

---

## ▶️ Usage

Run scripts in sequence using Spark:

<code>
spark-submit src/01_ingest.py
spark-submit src/02_explore_delta.py
spark-submit src/03_analyze.py
# Continue as needed...
</code>

You can also run individual tasks like:

<code>
spark-submit src/04_optimize.py
</code>

---

## 🧩 Challenges & Solutions

- **Data Skew**: Repartitioned during transformations  
- **Small File Problem**: Handled with Delta `OPTIMIZE`  
- **Schema Evolution**: Enabled with `mergeSchema` and autoMerge config  

---

## 🚀 Future Work

- Integrate with **Apache Airflow** for scheduling  
- Add **ML models** for predictive insights  
- Add **data quality checks** pre-ingestion  

---

## 🤝 Contributing

All contributions are welcome!

1. Fork this repository  
2. Create a new feature branch  
3. Submit a pull request with a clear description  

---

## 📄 License

Licensed under the **MIT License**. See `LICENSE` for more.
