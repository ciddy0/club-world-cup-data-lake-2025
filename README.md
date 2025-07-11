# Club World Cup 2025 Data Lake

This project builds a data ingestion pipeline for the 2025 FIFA Club World Cup using Apache Airflow and the ESPN public API. It fetches, stores, and prepares match and player data for further analysis.

## Project Structure

```
club-world-cup-data-lake-2025/
├── dags/
│ └── ingest_matches.py # Airflow DAG to ingest match data
├── scripts/
│ └── api_fetcher.py # Python script that fetches match data from ESPN API
| └── transform_data.py # Extracts and transforms raw JSON into structured records
| └── load_data.py # Load transformed data into Supabase tables
├── data/
│ └── raw/ # Raw JSON files saved from the API
├── logs/ # Airflow logs (ignored via .gitignore)
├── .env # Environemtn variables for Supabase credentials 
├── .gitignore
└── README.md
```

## Features

- Daily data ingestion using Airflow Dag (ingest_cwc_matches) schedualed at midnight (@daily)
- ESPN API integration for real-time Club World Cup match data
- Raw data stored as timestamped JSON files
- Extraction and denormalization of team stats, player stats, and rosters
- Automatic upsert into Supabase for matches, teams, team_stats, players and player_stats

## Environment Variables

Create a .env file in the project root with the following
```
# Supabase credentials
SUPABASE_URL=https://<your-project>.supabase.co
SUPABASE_SERVICE_KEY=<service-role-key>
```
Load these variables in your enviroment before running Airflow
```
export SUPABASE_URL=https://<your-project>.supabase.co
export SUPABASE_SERVICE_KEY=<service-role-key>
```

# Airflow DAG: ingest_cwc_macthes.py

Located in dags/, this DAG defines three tasks:
1. Fetch_match_data: calls api_fetcher.fetch_match_data to pull yesterday's scoreboard data and save raw JSON in data/raw.
2. fetch_match_summaries: Calls api_fetcher.fetch_match_summaries to fetch per-match summaries for event ID and save in data/raw
3. load_all_matches_to_supabase: Calls load_data.load_all_matches_to_supabase which uses transform_data.extract_summary_data_from_files and load_data.load_to_supabase to persist data in Supabase.
The Dag begins on 2025-06-14, retries once on failure, and will not backfill past runs.

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
