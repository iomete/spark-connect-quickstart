# Spark Connect Quickstart

Examples for connecting to a remote Spark cluster using Spark Connect — in Python, R, and Jupyter.

## Prerequisites

- Python 3.8+ or R 4.x
- A Spark Connect cluster endpoint (`sc://<host>:<port>`)

---

## Python Setup

```shell
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Example 1 — Basic DataFrame operations

**File:** `example.py`

**Change:** Replace the connection string on line 12:
```
sc://...  →  sc://<your-host>:<port>
```

```shell
python example.py
```

---

## Example 2 — Custom SSL/TLS certificate

**File:** `example_custom_cert.py`

**Changes required:**
1. Place your CA certificate file in the project root as `ca-cert-chain.crt`
2. Line 13 — replace the host with your actual endpoint:
   ```
   "example.iomete.com:443"  →  "<your-host>:<port>"
   ```
3. Line 16 — replace the connection string:
   ```
   sc://...  →  sc://<your-host>:<port>
   ```

```shell
python example_custom_cert.py
```

---

## Example 3 — Jupyter Notebook

**File:** `example.ipynb`

Open in JupyterLab, VS Code, or PyCharm.

**Change:** Update the connection string in the `SparkSession` cell, same as Example 1.

```shell
jupyter notebook
```

---

## Example 4 — EDA: Hashtags, Sentiment, User Activity

**File:** `eda/example.py`

**Changes required:**
1. Update the connection string (same as Example 1)
2. Replace the access token placeholder with your token

```shell
cd eda
pip install -r requirements.txt
python example.py
```

---

## Example 5 — R with sparklyr

**File:** `example.R`

Connects via sparklyr and demonstrates three patterns:
1. Upload a local R dataframe to Spark with `copy_to()`
2. Query a remote Spark table using `dplyr` verbs
3. Run raw SQL via the DBI interface

**Change:** Replace the connection string in the `spark_connect()` call:
```
sc://...  →  sc://<your-host>:<port>
```

### Setup

```r
install.packages("renv")
renv::restore()
```

```shell
Rscript example.R
```

---

## Project Structure

```
spark-connect-quickstart/
├── example.py                  # Basic PySpark example
├── example_custom_cert.py      # PySpark with custom SSL certificate
├── example.ipynb               # Jupyter notebook
├── example.R                   # R / sparklyr example
├── requirements.txt            # Python dependencies
├── renv.lock                   # R package lock file
└── eda/
    ├── example.py              # EDA: hashtags, sentiment, user activity
    └── requirements.txt
```