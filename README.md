# Stock-Market-Data-Engineering-Project

## 📌 Project Summary

A fully containerized, end-to-end real-time data engineering pipeline that simulates live stock market data, streams it through Apache Kafka, stores it on AWS S3, and makes it queryable via AWS Glue and Athena — all visualized through a live Streamlit dashboard.

This project demonstrates a production-style data pipeline architecture using industry-standard tools.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE FLOW                            │
│                                                                      │
│  CSV Dataset                                                         │
│  (indexProcessed.csv)                                                │
│       │                                                              │
│       ▼                                                              │
│  ┌─────────────┐    Kafka Topic     ┌──────────────┐                │
│  │   Producer  │ ─────────────────► │   Consumer   │                │
│  │  (Python)   │   stock_market     │   (Python)   │                │
│  └─────────────┘                    └──────┬───────┘                │
│                                            │                        │
│                                            ▼                        │
│                                     ┌──────────────┐               │
│                                     │    AWS S3    │               │
│                                     │  (Raw Store) │               │
│                                     └──────┬───────┘               │
│                                            │                        │
│                          ┌─────────────────┼──────────────┐        │
│                          ▼                 ▼              ▼        │
│                    ┌──────────┐     ┌──────────┐   ┌──────────┐   │
│                    │AWS Glue  │     │  Athena  │   │Streamlit │   │
│                    │(Catalog) │────►│ (Query)  │   │Dashboard │   │
│                    └──────────┘     └──────────┘   └──────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer           | Technology                        |
|-----------------|-----------------------------------|
| Language        | Python 3.10                       |
| Message Broker  | Apache Kafka                      |
| Cloud Storage   | AWS S3                            |
| Data Catalog    | AWS Glue                          |
| Query Engine    | AWS Athena                        |
| Dashboard       | Streamlit                         |
| Containerization| Docker & Docker Compose           |
| Dataset         | Real stock market CSV data        |

---

## ✨ Key Features

- **Real-time streaming** — Producer reads stock data and pushes to Kafka every second
- **Fault-tolerant ingestion** — Consumer reliably writes streamed data to AWS S3
- **Serverless querying** — AWS Glue crawls S3 and Athena enables SQL queries on live data
- **Live dashboard** — Streamlit app displays real-time stock prices and trends
- **Fully containerized** — Docker Compose brings up Kafka + Zookeeper with a single command

---

## 📁 Project Structure

```
stock-market-kafka-data-engineering-project/
│
├── stock_producer.py          # Kafka producer — reads CSV and streams data
├── stock_consumer.py          # Kafka consumer — receives and stores to S3
├── streamlit_kafka.py         # Streamlit dashboard for real-time visualization
├── docker-compose.yml         # Kafka + Zookeeper container setup
├── requirements.txt           # Python dependencies
├── requirements_fixed.txt     # Pinned dependency versions
├── indexProcessed.csv         # Stock market dataset
├── data_flow_diagram.png      # Visual architecture diagram
├── system_architecture.png    # System design reference
└── README.md
```

---

## ⚙️ Setup & Installation

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- AWS account with S3, Glue, and Athena configured
- AWS CLI configured (`aws configure`)

### Step 1 — Clone the Repository
```bash
git clone https://github.com/Farihakk67/Stock-Market-Data-Engineering-Project.git
cd Stock-Market-Data-Engineering-Project
```

### Step 2 — Start Kafka with Docker
```bash
docker-compose up -d
```
This starts Kafka and Zookeeper. Wait ~15 seconds before proceeding.

### Step 3 — Install Python Dependencies
```bash
pip install -r requirements_fixed.txt
```

### Step 4 — Run the Producer
```bash
python stock_producer.py
```
This begins streaming stock data into the Kafka topic `stock_market`.

### Step 5 — Run the Consumer (separate terminal)
```bash
python stock_consumer.py
```
This reads from Kafka and writes data to your configured S3 bucket.

### Step 6 — Launch the Dashboard
```bash
streamlit run streamlit_kafka.py
```
Open `http://localhost:8501` to view the live dashboard.

---

## ☁️ AWS Setup Notes

1. Create an S3 bucket (e.g., `stock-market-pipeline-data`)
2. Create a Glue Crawler pointing to the S3 bucket
3. Run the crawler to populate the Glue Data Catalog
4. Use Athena to query with SQL:
```sql
SELECT * FROM stock_market_db.stock_data
ORDER BY timestamp DESC
LIMIT 100;
```

---

## 📊 Dataset

- **File:** `indexProcessed.csv`
- **Size:** ~10 MB
- **Content:** Historical stock market index data including open, close, high, low, volume

---

## 🤝 Contributing

Contributions are welcome.

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m "Add your feature"`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## 👩‍💻 Author

**Fariha**
BS Computer Science — Muhammad Ali Jinnah University, Karachi
[GitHub](https://github.com/Farihakk67) • [LinkedIn](https://linkedin.com/in/fariha-kk)
