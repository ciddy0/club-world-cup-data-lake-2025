import requests
import os
from datetime import datetime, timedelta
import json
import logging

def fetch_match_data(**context):
    logging.info("Fetching match data from ESPN API...")
    
    raw_data_dir = "/opt/airflow/data/raw"
    os.makedirs(raw_data_dir, exist_ok=True)

    date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
    url = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.cwc/scoreboard"
    params = {"dates": date}

    print(f"Fetching ESPN data for date: {date}")
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        file_path = os.path.join(raw_data_dir, f"matches_{timestamp}.json")

        print(f"Saving file to: {file_path}")
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        logging.info(f"Saved raw data to {file_path}")

        # Extract match IDs and push to XCom
        event_ids = [event["id"] for event in data.get("events", [])]
        context['ti'].xcom_push(key="event_ids", value=event_ids)
    else:
        logging.error(f"Failed to fetch ESPN data: {response.status_code} {response.text}")
        response.raise_for_status()

def fetch_match_summaries(**context):
    logging.info("Fetching match summaries...")

    event_ids = context['ti'].xcom_pull(key="event_ids", task_ids="fetch_match_data")
    if not event_ids:
        logging.warning("No match IDs found to fetch summaries for.")
        return

    raw_data_dir = "/opt/airflow/data/raw"
    os.makedirs(raw_data_dir, exist_ok=True)

    for event_id in event_ids:
        url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.cwc/summary"
        params = {"event": event_id}
        response = requests.get(url, params=params)

        if response.status_code == 200:
            summary_data = response.json()
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            file_path = os.path.join(raw_data_dir, f"summary_{event_id}_{timestamp}.json")

            print(f"Saving summary to: {file_path}")
            with open(file_path, "w") as f:
                json.dump(summary_data, f, indent=2)

            logging.info(f"Saved summary for event {event_id}")
        else:
            logging.error(f"Failed to fetch summary for event {event_id}: {response.status_code}")

def extract_summary_data_from_files(event_ids, raw_data_dir="/opt/airflow/data/raw"):
    all_match_data = []

    for event_id in event_ids:
        matching_files = [
            f for f in os.listdir(raw_data_dir)
            if f.startswith(f"summary_{event_id}_") and f.endswith(".json")
        ]
        if not matching_files:
            print(f"No summary file found for event {event_id}")
            continue
        
        matching_files.sort(reverse=True)
        file_path = os.path.join(raw_data_dir, matching_files[0])

        with open(file_path, "r") as f:
            data = json.load(f)

        match_info = {
            "date": data.get("date"),
            "home_team": data.get("home_team", {}).get("displayName"),
            "away_team": data.get("away_team", {}).get("displayName"),
            "home_team_id": data.get("home_team", {}).get("id"),
            "away_team_id": data.get("away_team", {}).get("id")
        }

        results = {
            "rosters": [],
            "team_stats": []
        }

        # Extract team stats as you already have it
        boxscore_teams = data.get("boxscore", {}).get("teams", [])
        for team_entry in boxscore_teams:
            team = team_entry.get("team", {})
            stats = team_entry.get("statistics", [])
            stats_dict = {s["name"]: s["displayValue"] for s in stats}

            results["team_stats"].append({
                "teamId": team.get("id"),
                "teamName": team.get("displayName"),
                "abbreviation": team.get("abbreviation"),
                "logo": team.get("logo"),
                "stats": stats_dict
            })

        # Extract roster info as you have it
        for team_roster in data.get("rosters", []):
            team_info = team_roster["team"]
            home_away = team_roster["homeAway"]
            players = team_roster["roster"]

            for player in players:
                athlete = player.get("athlete", {})
                stats = player.get("stats", [])
                results["rosters"].append({
                    "team": team_info["displayName"],
                    "teamId": team_info["id"],
                    "homeAway": home_away,
                    "playerId": athlete.get("id"),
                    "fullName": athlete.get("fullName"),
                    "jersey": player.get("jersey"),
                    "starter": player.get("starter"),
                    "active": player.get("active"),
                    "subbedIn": player.get("subbedIn"),
                    "subbedOut": player.get("subbedOut"),
                    "stats": {stat["name"]: stat["value"] for stat in stats}
                })

        all_match_data.append({
            "match_id": event_id,
            "match_info": match_info,
            "results": results
        })

    return all_match_data

   
