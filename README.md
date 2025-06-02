# Club World Cup 2025 Data Lake

This project builds a data ingestion pipeline for the 2025 FIFA Club World Cup using Apache Airflow and the ESPN public API. It fetches, stores, and prepares match and player data for further analysis.

## Project Structure

club-world-cup-data-lake-2025/
├── dags/
│ └── ingest_matches.py # Airflow DAG to ingest match data
├── scripts/
│ └── api_fetcher.py # Python script that fetches match data from ESPN API
├── data/
│ └── raw/ # Raw JSON files saved from the API
├── logs/ # Airflow logs (ignored via .gitignore)
├── .gitignore
└── README.md

## Features

- Daily data ingestion using Airflow
- ESPN API integration for real-time Club World Cup match data
- Raw data stored as timestamped JSON files
- Easy extension to include player stats, team info, standings, and more

## ⚙️ Setup

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/club-world-cup-data-lake-2025.git
cd club-world-cup-data-lake-2025
```
### 2. Start Airflow

This project uses the official Airflow Docker image. You can start it using:

```bash
docker-compose up -d
```

Make sure volumes are mounted correctly so that data files are accessible on the host.

### 3. Trigger the DAG

Open the Airflow UI at http://localhost:8080, enable the iingest_cwc_matches DAG and trigger
it manually or let it run on schedule

## ESPN API

The project uses the following endpoint to fetch match data:

```bash
GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.cwc/scoreboard?dates=YYYYMMDD
```

## Output

Match data is stored under:

```pgsql
/data/raw/matches_<timestamp>.json
```

Each file contains match metadeta returned from the ESPN scoreboard endpoint
