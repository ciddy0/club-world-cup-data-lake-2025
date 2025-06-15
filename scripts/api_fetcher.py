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

def fetch_player_stats():
    date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
    url = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.cwc/scoreboard"
    params = {"dates": date}

    print(f"Fetching ESPN data for date: {date}")
    response = requests.get(url, params=params)
    scoreboard = response.json()
    events = scoreboard.get("events", [])
    summary_data = ""
    for event in events:
        event_id = event["id"]
        print("event id: ", event_id)
        summary_url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.cwc/summary?event={event_id}"
        summary_resp = requests.get(summary_url)
        if summary_resp.status_code == 200:
            summary_data = summary_resp.json()
    rosters = summary_data.get("rosters", [])
    all_rosters = []

    for team_roster in rosters:
        team_info = team_roster["team"]
        home_away = team_roster["homeAway"]
        players = team_roster["roster"]

        for player in players:
            athlete = player.get("athlete", {})
            stats = player.get("stats", [])
            player_data = {
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
            }
            all_rosters.append(player_data)

    return all_rosters




if __name__ == "__main__":
   roster = fetch_player_stats()
   if roster:
       count = 0
       for player in roster:
           print(f"{count+1}: {player['fullName']} ({player['team']}) - Jersey #{player['jersey']} - Stats: {player['stats']}")
           count += 1
   
