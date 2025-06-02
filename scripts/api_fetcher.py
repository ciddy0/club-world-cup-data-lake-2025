import requests
import os
from datetime import datetime
import json
import logging

def fetch_match_data():
    logging.info("Fetching match data from ESPN API...")
    
    # Make sure data/raw folder exists
    raw_data_dir = "/opt/airflow/data/raw"
    os.makedirs(raw_data_dir, exist_ok=True)

    # ESPN API endpoint for FIFA Club World Cup scoreboard
    date = datetime.utcnow().strftime("%Y%m%d")
    url = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.cwc/scoreboard"
    params = {"dates": date}

    print(f"Fetching ESPN data for date: {date}")
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        file_path = os.path.join(raw_data_dir, f"ESPN_matches_{timestamp}.json")

        print(f"Saving file to: {file_path}")
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        logging.info(f"Saved raw data to {file_path}")
    else:
        logging.error(f"Failed to fetch ESPN data: {response.status_code} {response.text}")
        response.raise_for_status()


if __name__ == "__main__":
    fetch_match_data()
   
